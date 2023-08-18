package com.huberlin;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.huberlin.events.ControlEvent;
import com.huberlin.events.Event;
import org.apache.flink.cep.CEP;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

public class DataStreamJob {
	private enum OutputMode {
		NormalOutput,
		LeftOrRightFallback,
		CombinedFallback,
		NoOutput
	}

	private static final Logger logger = LoggerFactory.getLogger(DataStreamJob.class);

	private static final Map<String, String> nodeIdToForwardingIp = new HashMap<>();

	//private static final Map<String, String> ipToIdMap = new HashMap<>();

	private static ForwardRuleProcessor forwardRules;

	private static int ownPort;

	private static String ownNodeId = "";

	private static void loadTopologyConfig(String configPath, String nodeId) {
		ObjectMapper mapper = new ObjectMapper();
		Map<String, TopologyConfig> config;
		try {
			config = mapper.readValue(new File(configPath), new TypeReference<>() {
			});
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		TopologyConfig ownConfig = null;
		for (Map.Entry<String, TopologyConfig> nodeConfig : config.entrySet()) {
			String id = nodeConfig.getKey();
			if (id.equals(nodeId)) {
				ownConfig = nodeConfig.getValue();
				break;
			}
		}

		if (ownConfig == null) {
			System.err.println("No network config found for node id " + nodeId);
			System.exit(1);
		}

		ownNodeId = nodeId;
		ownPort = ownConfig.getPort();
		nodeIdToForwardingIp.putAll(ownConfig.getForwardingTable());
	}

	public static void main(String[] args) throws Exception {
		EngineOptions options = EngineOptions.parse(args);
		if (options == null) {
			System.exit(1);
		}

		final Configuration flinkConfig = GlobalConfiguration.loadConfiguration(options.getFlinkConfigDir());
		StreamExecutionEnvironment env;
		if (options.getWithWebUi()) {
			env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(flinkConfig);
		}
		else {
			env = StreamExecutionEnvironment.getExecutionEnvironment(flinkConfig);
		}

		env.getConfig().disableGenericTypes();

		FileSystem.initialize(flinkConfig, null);

		env.setParallelism(1);

		// load the json file which contains information about the network topology and connections to be established
		// Also determines own node id
		loadTopologyConfig(options.getNetworkConfigPath(), options.getOwnNodeId());

		final QueryPlan plan = QueryPlan.fromFile(options.getPlanPath());
		forwardRules = ForwardRuleProcessor.fromPlan(plan, ownNodeId);

		// Flink Data Streams Setup
		RichSourceFunction<Event> source = new RichSourceFunction<Event>() {
			private volatile boolean isCancelled;
			private BlockingQueue<Event> merged_event_stream;

			public void handleClient(Socket socket) {
				try {
					BufferedReader input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
					String client_address = socket.getRemoteSocketAddress().toString();

					String firstLine = input.readLine();
					while (firstLine == null) {
						firstLine = input.readLine();
					}

					if (!firstLine.startsWith("hello |")) {
						logger.error("Invalid connection attempt from " + client_address);
						socket.close();
						return;
					}

					String sourceNodeId = firstLine.substring(firstLine.indexOf('|') + 1).trim();

					while (!isCancelled) {
						String message = input.readLine();

						if (message == null) {
							continue;
						}

						// check, if a client has sent his entire event stream..
						if (message.contains("end-of-the-stream")) {
							System.out.println("[EndOfStream] Reached the end of the stream for " + client_address);

							socket.close();
							return;
						}
						// .. else, simply process the incoming event accordingly
						else {
							if (!message.contains("|")) {
								continue;
							}

							Event event = Event.parse(message, sourceNodeId);

							merged_event_stream.put(event);
						}
					}

				} catch (IOException | InterruptedException e) {
					System.err.print("[Error] Client disconnected");
				}
			}

			@Override
			public void run(SourceContext<Event> sourceContext) throws Exception {
				while (!isCancelled) {
					// retrieve and remove the head of the queue (event stream)
					Event event = merged_event_stream.take();

					// process it with Flink
					sourceContext.collect(event);
				}
			}

			@Override
			public void cancel() {
				isCancelled = true;
			}

			@Override
			public void open(Configuration parameters) throws Exception {
				super.open(parameters);

				merged_event_stream = new LinkedBlockingQueue<>();

				Thread accepting_thread = new Thread(() -> {
					try {
						try (ServerSocket socket = new ServerSocket(ownPort)) {
							System.out.println("[Socket] Listening on port " + ownPort);
							while (!isCancelled) {
								Socket new_client = socket.accept();
								new Thread(() -> handleClient(new_client),
										"client-" + new_client.getRemoteSocketAddress().toString()).start();

								System.out.println(
										"New client " + new_client.getRemoteSocketAddress().toString() + " connected");
							}
						}
					} catch (IOException e) {
						System.err.print("[Error] Unacceptable: " + e);
					}
				}, "accepting-thread");
				accepting_thread.start();
			}
		};

		DataStream<Event> inputStream = env.addSource(source, "socket-source");

		// Print for monitor
		inputStream.print().name("input-print");

		final OutputTag<Event> forwardOutputTag = new OutputTag<>("forward") {
		};

		IterativeStream<Event> iteration = inputStream.iterate();
		iteration.name("event-iteration");

		SingleOutputStreamOperator<Event> splitStream = iteration
			.assignTimestampsAndWatermarks(new CustomWatermarkStrategy())
			.process(new ProcessFunction<Event, Event>() {
				@Override
				public void processElement(Event value, ProcessFunction<Event, Event>.Context ctx, Collector<Event> out) {
					if (forwardRules.shouldForward(value)) {
						// Output events that need to be forwarded are output using side output
						ctx.output(forwardOutputTag, value);
					}

					if (value.isControl("change_rules")) {
						ControlEvent control = (ControlEvent)value;
						String[] params = control.getParameters();
						Collection<String> replacements = Arrays.stream(params).skip(3).collect(Collectors.toList());
						forwardRules.replaceForwardRules(params[0], params[1], params[2], replacements, false);
					}

					out.collect(value);
				}
			})
			.name("split-stream");

		// apply patterns to data stream
		List<SingleOutputStreamOperator<Event>> matchStreams = new ArrayList<>();
		List<DataStream<Event>> partialMatchForwardStreams = new ArrayList<>();

		OutputTag<Event> partialMatchForwardTag = new OutputTag<>("forwardPartialMatch") {
		};

		for (QueryInformation.Processing query : plan.nodes.get(ownNodeId).queries.values()) {
			String patternName = query.query_name;
			if (options.getNoSelectivity()) {
				query.predicate_checks = 0;
			}

			FallbackPattern fallbackPattern = FallbackPattern.fromQuery(query);
			boolean hasCombinedFallback = fallbackPattern.getCombinedFallback() != null;

			// Required because of serialization
			Set<String> possibleFallbacks = fallbackPattern.getPossibleFallbacks();

			System.out.println("Creating pattern " + patternName + " FROM " + query.inputs);

			OutputTag<Event> fallbackOutputTag = new OutputTag<>("fallback-" + patternName) {
			};
			OutputTag<Event> combinedFallbackOutputTag = new OutputTag<>("combined-fallback-" + patternName) {};
			OutputTag<Event> partialMatchOutputTag = new OutputTag<>("partialMatch-" + patternName) {
			};

			// The process function outputs events into patternInput until a control message for this pattern
			// occurs in the event stream. Depending on the control message type, events are either output using a
			// side output or discarded (only for this pattern) The control message is the last event in the normal output.
			SingleOutputStreamOperator<Event> patternInput = splitStream.process(new ProcessFunction<Event, Event>() {
				private EnumSet<OutputMode> outputMode = EnumSet.of(OutputMode.NormalOutput);
				private ControlEvent previousTrigger = null;

				@Override
				public void processElement(Event value, ProcessFunction<Event, Event>.Context ctx, Collector<Event> out) {
					if (value.isControl()) {
						ControlEvent control = (ControlEvent)value;
						switch (control.getEventType()) {
							case "switch_pattern":
								// Occurs when other pattern has rate change
								// Switch from the pattern that receives the input specified in the control parameter
								// to a pattern that uses the parts of the old input as inputs instead.
								String targetFallback = control.getParameter();
								String targetPattern = control.getParameter(1);
								if (patternName.equals(targetPattern) && possibleFallbacks.contains(targetFallback)) {
									if (previousTrigger == null) {
										// First switch_pattern for this pattern
										outputMode = EnumSet.of(OutputMode.LeftOrRightFallback);
									} else {
										if (!previousTrigger.getParameter().equals(targetFallback)) {
											if (hasCombinedFallback) {
												// Two different switch_pattern's for this pattern -> combined fallback
												outputMode = EnumSet.of(OutputMode.CombinedFallback);
											}
											else {
												logger.warn("Invalid switch_pattern for pattern " + patternName);
											}
										}
										else {
											// Duplicate switch_pattern command
											outputMode = EnumSet.of(OutputMode.LeftOrRightFallback);
										}
									}

									if (!control.getParameter(2).equals("disable")) {
										outputMode.add(OutputMode.NormalOutput);
									}

									System.out.println("[Control] Switching combination of " + patternName + " for input " + targetFallback + ". New mode: " + outputMode);
									previousTrigger = control;
								}
								break;
							case "forward_inputs":
								// Disable the pattern specified in the control parameter and forward its inputs instead
								if (control.getParameter().equals(patternName)) {
									System.out.println("[Control] Forwarding inputs " + query.inputs + " instead of " + patternName);

									boolean removeOld;
									if (control.getParameter(1).equals("disable")) {
										outputMode = EnumSet.of(OutputMode.NoOutput);
										removeOld = false;
									}
									else {
										outputMode = EnumSet.of(OutputMode.NormalOutput);
										removeOld = true;
									}


									forwardRules.replaceForwardRules(patternName, ownNodeId, ForwardRuleProcessor.REPLACE_MODE_PROJECTION, query.inputs, removeOld);
								}
								break;
						}

						ctx.output(fallbackOutputTag, value);
						out.collect(value);
					}
					else {
						if (outputMode.contains(OutputMode.NormalOutput)) {
							out.collect(value);
						}

						if (outputMode.contains(OutputMode.CombinedFallback)) {
							ctx.output(combinedFallbackOutputTag, value);
						}
						else if (outputMode.contains(OutputMode.LeftOrRightFallback)) {
							ctx.output(fallbackOutputTag, value);
						}
					}
				}
			}).name(patternName + "-input");

			OutputTag<Event> lateOutputTag = new OutputTag<>("late" + patternName) {
			};

			// Apply pattern to input stream and process complex matches
			// If a control message is matched, the current partial matches are output using a side output.
			SingleOutputStreamOperator<Event> stream = CEP.pattern(patternInput, fallbackPattern.getNormal())
					.sideOutputLateData(lateOutputTag)
					.process(new ComplexEventProcessFunction(patternName, ownNodeId, query.inputs.toString(), partialMatchOutputTag, partialMatchForwardTag))
					.name(patternName + "-process");

			stream.getSideOutput(lateOutputTag).addSink(new RichSinkFunction<>() {
				@Override
				public void invoke(Event value, Context context) {
					System.out.println("[Late " + patternName + "] " + value);
				}
			}).name(patternName + "-late");

			matchStreams.add(stream);
			partialMatchForwardStreams.add(stream.getSideOutput(partialMatchForwardTag));

			DataStream<Event> partialMatchStream = stream.getSideOutput(partialMatchOutputTag);
			DataStream<Event> fallbackInput = partialMatchStream.union(patternInput.getSideOutput(fallbackOutputTag))
					.process(new ProcessFunction<>() {
						private boolean isFallbackEnabled = false;

						@Override
						public void processElement(Event value, ProcessFunction<Event, Event>.Context ctx, Collector<Event> out) {
							if (value.isControl()) {
								ControlEvent controlEvent = (ControlEvent) value;
								if (value.getEventType().equals("switch_pattern") &&
										possibleFallbacks.contains(controlEvent.getParameter()) &&
										controlEvent.getParameter(1).equals(patternName)) {
									isFallbackEnabled = !isFallbackEnabled;
									System.out.println("Changed fallback input mode for " + patternName + " to " + isFallbackEnabled);
								}
							}

							if (value.isControl() || isFallbackEnabled) {
								out.collect(value);
							}
						}
					});


			List<DataStream<Event>> combinedFallback = new ArrayList<>(fallbackPattern.getFallbacks().size());

			// Setup fallback patterns
			for (Map.Entry<String, FallbackPattern.FallbackQuery> fallback : fallbackPattern.getFallbacks().entrySet()) {
				String trigger = fallback.getKey();
				FallbackPattern.FallbackQuery fallbackQuery = fallback.getValue();

				System.out.println("  - Fallback for " + trigger + ": " + patternName + " FROM " + fallbackQuery.getQuery().inputs + ", Combined trigger: " + fallbackQuery.getCombinedTrigger());

				ComplexEventProcessFunction fallbackProcessFunction;
				if (!hasCombinedFallback) {
					fallbackProcessFunction = new ComplexEventProcessFunction(patternName, ownNodeId, fallbackQuery.getQuery().inputs.toString(), null, null);
				}
				else {
					fallbackProcessFunction = new ComplexEventProcessFunction(patternName, ownNodeId, fallbackQuery.getQuery().inputs.toString(), fallbackOutputTag, partialMatchForwardTag);
				}

				SingleOutputStreamOperator<Event> fallbackStream = CEP.pattern(fallbackInput, fallbackQuery.getFallback())
						.process(fallbackProcessFunction)
						.name(patternName + "-fallback-" + trigger);

				partialMatchForwardStreams.add(fallbackStream.getSideOutput(partialMatchForwardTag));
				matchStreams.add(fallbackStream);

				if (hasCombinedFallback) {
					combinedFallback.add(fallbackStream.getSideOutput(fallbackOutputTag));
				}
			}

			if (combinedFallback.size() > 0) {
				@SuppressWarnings("unchecked")
				DataStream<Event> combinedFallbackInput = partialMatchStream
						.union(patternInput.getSideOutput(combinedFallbackOutputTag))
						.union(combinedFallback.toArray(new DataStream[0]))
						.filter(new DuplicateEventFilter());

				FallbackPattern.FallbackQuery combinedFallbackQuery = fallbackPattern.getCombinedFallback();

				SingleOutputStreamOperator<Event> combinedFallbackStream = CEP.pattern(combinedFallbackInput, combinedFallbackQuery.getFallback())
						.process(new ComplexEventProcessFunction(patternName, ownNodeId, combinedFallbackQuery.getQuery().inputs.toString(), null, null))
						.name(patternName + "-fallback-combined");

				matchStreams.add(combinedFallbackStream);

				System.out.println("  - Combined fallback: " + patternName + " FROM " + combinedFallbackQuery.getQuery().inputs);
			}
		}

		// Union of all match streams
		if (matchStreams.size() > 0) {
			@SuppressWarnings("unchecked")
			DataStream<Event> outputStream = matchStreams.get(0)
					.union(matchStreams.subList(1, matchStreams.size()).toArray(new DataStream[0]));

			// Complex event matches flow back to the start of the iteration
			iteration.closeWith(outputStream);

			outputStream.addSink(new RichSinkFunction<>() {
				@Override
				public void invoke(Event value, Context context) {
					// Prefixed print for monitor
					System.out.println("[Match] " + value.toString());
				}
			}).name("match-print");
		}
		else {
			iteration.closeWith(splitStream.process(new ProcessFunction<>() {
				@Override
				public void processElement(Event value, ProcessFunction<Event, Event>.Context ctx, Collector<Event> out) {
					// No-op to close iteration
				}
			}));
		}

		@SuppressWarnings("unchecked")
		DataStream<Event> forwardEventStream = splitStream.getSideOutput(forwardOutputTag)
				.union(partialMatchForwardStreams.toArray(new DataStream[0]));

		forwardEventStream.addSink(new RichSinkFunction<>() {
			@Override
			public void open(Configuration parameters) {
				for (String dest : forwardRules.getAllDestinations()) {
					this.sentEvents.put(dest, new HashSet<>());
				}
			}

			@Override
			public void invoke(Event event, Context context) {
				Collection<String> destinations = forwardRules.getForwardDestinations(event);
				if (destinations != null) {
					String eventId = event.getEventId();
					List<String> finalDest = new ArrayList<>(destinations.size());
					for (String dest : destinations) {
						if (!event.getSourceNodeId().equals(dest) && !sentEvents.get(dest).contains(eventId)) {
							forward(event, nodeIdToForwardingIp.get(dest));
							sentEvents.get(dest).add(eventId);
							finalDest.add(dest);
						}
					}

					System.out.println("[Forward] " + event.getEventType() + " (" + event.getEventId() + ") -> " + finalDest);
				}
			}

			@Override
			public void close() throws Exception {
				for (PrintWriter conn : connections.values()) {
					conn.close();
				}

				for (Socket socket : sockets) {
					socket.close();
				}
			}

			private void forward(Event event, String target_ip) {
				try {
					// if the connection to a forwarding target was not established yet then establish it
					if (!connections.containsKey(target_ip)) {
						String[] ip = target_ip.split(":");

						Socket client_socket = new Socket(ip[0], Integer.parseInt(ip[1]));
						PrintWriter writer = new PrintWriter(client_socket.getOutputStream(), true);

						connections.put(target_ip, writer);
						sockets.add(client_socket);

						writer.write("hello | " + ownNodeId + "\n");

						System.out.println("Connection for forwarding events to " + target_ip + " established");
					}

					connections.get(target_ip).println(event.toString());
				} catch (Exception e) {
					logger.error("Error while forwarding event", e);
					System.err.println("[Error]: " + e + " - Event:" + event + " to " + target_ip);
				}
			}

			private final Map<String, PrintWriter> connections = new HashMap<>();
			private final HashMap<String, HashSet<String>> sentEvents = new HashMap<>();
			private final Collection<Socket> sockets = new ArrayList<>();
		}).name("forward-sink");

		// Start cluster/CEP-engine
		env.execute("FlinkCEP Node " + ownNodeId);
	}
}
