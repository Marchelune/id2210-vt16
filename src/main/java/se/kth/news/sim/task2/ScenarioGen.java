package se.kth.news.sim.task2;

/*
 * 2016 Royal Institute of Technology (KTH)
 *
 * LSelector is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import se.kth.news.sim.ScenarioSetup;
import se.kth.news.sim.compatibility.SimNodeIdExtractor;
import se.kth.news.sim.task3.SimulationObserverTask3_1;
import se.kth.news.sim.task3.SimulationObserverTask3_2;
import se.kth.news.sim.task4.SimulationObserverTask4_1;
import se.kth.news.sim.task4.leaderStore;
import se.kth.news.system.HostMngrComp;
import se.sics.kompics.Init;
import se.sics.kompics.network.Address;
import se.sics.kompics.simulator.SimulationScenario;
import se.sics.kompics.simulator.adaptor.Operation;
import se.sics.kompics.simulator.adaptor.Operation1;
import se.sics.kompics.simulator.adaptor.Operation2;
import se.sics.kompics.simulator.adaptor.Operation3;
import se.sics.kompics.simulator.adaptor.distributions.ConstantDistribution;
import se.sics.kompics.simulator.adaptor.distributions.IntegerUniformDistribution;
import se.sics.kompics.simulator.adaptor.distributions.extra.BasicIntSequentialDistribution;
import se.sics.kompics.simulator.events.system.KillNodeEvent;
import se.sics.kompics.simulator.events.system.SetupEvent;
import se.sics.kompics.simulator.events.system.StartNodeEvent;
import se.sics.kompics.simulator.network.identifier.IdentifierExtractor;
import se.sics.kompics.simulator.util.GlobalView;
import se.sics.ktoolbox.omngr.bootstrap.BootstrapServerComp;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.overlays.id.OverlayIdRegistry;
import static java.lang.Math.toIntExact;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class ScenarioGen {
	 static Operation setupOp = new Operation<SetupEvent>() {
	        public SetupEvent generate() {
	            return new SetupEvent() {
	                @Override
	                public void setupGlobalView(GlobalView gv) {
	                		// Store to save the maximum gradient convergence time for each node
	                		gv.setValue("simulation.maxCvTimeStore", new MaxCvTimeStore());
	                		// Task 3.1
	                		// Store to save the number of pull rounds needed for a node to get the leader info
	                		gv.setValue("simulation.gotLeaderStore", new MaxCvTimeStore());
	                		// Store to save the number of pull rounds of a node when the leader is elected
	                		gv.setValue("simulation.nbRoundsWhenLeaderElectStore", new MaxCvTimeStore());
	                		// Store to save the current number of pull round of the nodes
	                		gv.setValue("simulation.nbPullRoundsStore", new MaxCvTimeStore());
	                		// Task 3.2
	                		// Store to save how many news items a node has in its news chain
	                		gv.setValue("simulation.nbNewsStore", new MaxCvTimeStore());
	                		// Task 4.1
	                		// Store to save the leader for each node
	                		gv.setValue("simulation.leaderStore", new leaderStore());
	                		// Store to save which nodes detected a leader failure
	                		gv.setValue("simulation.leaderFailureStore", new MaxCvTimeStore());
	                }
	            };
	        }
	    };
	    
	    static Operation1 startObserverTask2Op = new Operation1<StartNodeEvent, Long>() {
	        public StartNodeEvent generate(final Long nodeId) {
	            return new StartNodeEvent() {
	            	KAddress selfAdr;
	            	int nodeIdInt = toIntExact(nodeId);

	                {
	                	selfAdr = ScenarioSetup.getNodeAdr(nodeIdInt);
	                }

	                @Override
	                public Map<String, Object> initConfigUpdate() {
	                    HashMap<String, Object> config = new HashMap<String, Object>();
	                    config.put("simulation.checktimeout", 1000);
	                    return config;
	                }
	                
	                @Override
	                public Address getNodeAddress() {
	                    return selfAdr;
	                }

	                @Override
	                public Class getComponentDefinition() {
	                    return SimulationObserverTask2.class;
	                }

	                @Override
	                public Init getComponentInit() {
	                    return Init.NONE;
	                }
	            };
	        }
	    };
	    
	    static Operation1 startObserverTask3_1Op = new Operation1<StartNodeEvent, Long>() {
	        public StartNodeEvent generate(final Long nodeId) {
	            return new StartNodeEvent() {
	            	KAddress selfAdr;
	            	int nodeIdInt = toIntExact(nodeId);

	                {
	                	selfAdr = ScenarioSetup.getNodeAdr(nodeIdInt);
	                }

	                @Override
	                public Map<String, Object> initConfigUpdate() {
	                    HashMap<String, Object> config = new HashMap<String, Object>();
	                    config.put("simulation.checktimeout", 1);
	                    return config;
	                }
	                
	                @Override
	                public Address getNodeAddress() {
	                    return selfAdr;
	                }

	                @Override
	                public Class getComponentDefinition() {
	                    return SimulationObserverTask3_1.class;
	                }

	                @Override
	                public Init getComponentInit() {
	                    return Init.NONE;
	                }
	            };
	        }
	    };
	    
	    static Operation1 startObserverTask3_2Op = new Operation1<StartNodeEvent, Long>() {
	        public StartNodeEvent generate(final Long nodeId) {
	            return new StartNodeEvent() {
	            	KAddress selfAdr;
	            	int nodeIdInt = toIntExact(nodeId);

	                {
	                	selfAdr = ScenarioSetup.getNodeAdr(nodeIdInt);
	                }

	                @Override
	                public Map<String, Object> initConfigUpdate() {
	                    HashMap<String, Object> config = new HashMap<String, Object>();
	                    config.put("simulation.checktimeout", 1000);
	                    return config;
	                }
	                
	                @Override
	                public Address getNodeAddress() {
	                    return selfAdr;
	                }

	                @Override
	                public Class getComponentDefinition() {
	                    return SimulationObserverTask3_2.class;
	                }

	                @Override
	                public Init getComponentInit() {
	                    return Init.NONE;
	                }
	            };
	        }
	    };
	    
	    static Operation1 startObserverTask4_1Op = new Operation1<StartNodeEvent, Long>() {
	        public StartNodeEvent generate(final Long nodeId) {
	            return new StartNodeEvent() {
	            	KAddress selfAdr;
	            	int nodeIdInt = toIntExact(nodeId);

	                {
	                	selfAdr = ScenarioSetup.getNodeAdr(nodeIdInt);
	                }

	                @Override
	                public Map<String, Object> initConfigUpdate() {
	                    HashMap<String, Object> config = new HashMap<String, Object>();
	                    config.put("simulation.checktimeout", 1);
	                    return config;
	                }
	                
	                @Override
	                public Address getNodeAddress() {
	                    return selfAdr;
	                }

	                @Override
	                public Class getComponentDefinition() {
	                    return SimulationObserverTask4_1.class;
	                }

	                @Override
	                public Init getComponentInit() {
	                    return Init.NONE;
	                }
	            };
	        }
	    };

 static Operation<SetupEvent> systemSetupOp = new Operation<SetupEvent>() {
     @Override
     public SetupEvent generate() {
         return new SetupEvent() {
             @Override
             public void setupSystemContext() {
                 OverlayIdRegistry.registerPrefix("newsApp", ScenarioSetup.overlayOwner);
             }

             @Override
             public IdentifierExtractor getIdentifierExtractor() {
                 return new SimNodeIdExtractor();
             }
         };
     }
 };

 static Operation<StartNodeEvent> startBootstrapServerOp = new Operation<StartNodeEvent>() {

     @Override
     public StartNodeEvent generate() {
         return new StartNodeEvent() {
             KAddress selfAdr;

             {
                 selfAdr = ScenarioSetup.bootstrapServer;
             }

             @Override
             public Address getNodeAddress() {
                 return selfAdr;
             }

             @Override
             public Class getComponentDefinition() {
                 return BootstrapServerComp.class;
             }

             @Override
             public BootstrapServerComp.Init getComponentInit() {
                 return new BootstrapServerComp.Init(selfAdr);
             }
         };
     }
 };


 static Operation3<StartNodeEvent, Integer, Integer, Long> startNodeOp = new Operation3<StartNodeEvent, Integer, Integer, Long>() {

     @Override
     public StartNodeEvent generate(final Integer nodeId, final Integer timer, final Long writer) {
         return new StartNodeEvent() {
             KAddress selfAdr;

             {
                 selfAdr = ScenarioSetup.getNodeAdr(nodeId);
             }

             @Override
             public Address getNodeAddress() {
                 return selfAdr;
             }

             @Override
             public Class getComponentDefinition() {
                 return HostMngrComp.class;
             }

             @Override
             public HostMngrComp.Init getComponentInit() {
                 return new HostMngrComp.Init(selfAdr, ScenarioSetup.bootstrapServer, ScenarioSetup.newsOverlayId);
             }

             @Override
             public Map<String, Object> initConfigUpdate() {
                 Map<String, Object> nodeConfig = new HashMap<>();
                 nodeConfig.put("system.id", nodeId);
                 nodeConfig.put("system.seed", ScenarioSetup.getNodeSeed(nodeId));
                 nodeConfig.put("system.port", ScenarioSetup.appPort);
                 nodeConfig.put("newsTimeOut", timer);
                 // writer is 1 if the node is a writer, 0 otherwise
                 nodeConfig.put("writer", writer);
                 return nodeConfig;
             }
         };
     }
 };
 
 static Operation1 killNodeOp = new Operation1<KillNodeEvent, Long>() {
     @Override
     public KillNodeEvent generate(final Long nodeId) {
             return new KillNodeEvent() {
	            	KAddress selfAdr;
	            	int nodeIdInt = toIntExact(nodeId);

	                {
	                	selfAdr = ScenarioSetup.getNodeAdr(nodeIdInt);
	                }
                     @Override
                     public Address getNodeAddress() {
                             return selfAdr;
                     }

                     @Override
                     public String toString() {
                             return "KillNode<" + selfAdr.toString() + ">";
                     }
             };
     }
};
 
 public static SimulationScenario simpleBoot() {
 	final Random rnd = new Random();
     SimulationScenario scen = new SimulationScenario() {
         {
             StochasticProcess setup = new SimulationScenario.StochasticProcess() {
                 {
                     raise(1, setupOp);
                 }
             };
             
             StochasticProcess systemSetup = new StochasticProcess() {
                 {
                     eventInterArrivalTime(constant(1000));
                     raise(1, systemSetupOp);
                 }
             };
             StochasticProcess startBootstrapServer = new StochasticProcess() {
                 {
                     eventInterArrivalTime(constant(1000));
                     raise(1, startBootstrapServerOp);
                 }
             };
             
             SimulationScenario.StochasticProcess startObserverTask2 = new SimulationScenario.StochasticProcess() {
                 {
                 	eventInterArrivalTime(constant(1000));
                     raise(1, startObserverTask2Op, constant(0));
                 }
             };
             
             SimulationScenario.StochasticProcess startObserverTask3_1 = new SimulationScenario.StochasticProcess() {
                 {
                 	eventInterArrivalTime(constant(1000));
                     raise(1, startObserverTask3_1Op, constant(0));
                 }
             };
             
             SimulationScenario.StochasticProcess startObserverTask3_2 = new SimulationScenario.StochasticProcess() {
                 {
                 	eventInterArrivalTime(constant(1000));
                     raise(1, startObserverTask3_2Op, constant(0));
                 }
             };
             
             SimulationScenario.StochasticProcess startObserverTask4_1 = new SimulationScenario.StochasticProcess() {
                 {
                 	eventInterArrivalTime(constant(1000));
                     raise(1, startObserverTask4_1Op, constant(0));
                 }
             };
             StochasticProcess startNonWriterPeers = new StochasticProcess() {
                 {
                 	
                     eventInterArrivalTime(uniform(1, 50));

                     raise(188, startNodeOp, new BasicIntSequentialDistribution(63), new IntegerUniformDistribution(1000,5000,rnd), constant(0));
                 }
             };
             StochasticProcess startWriterPeers = new StochasticProcess() {
                 {
                 	
                     eventInterArrivalTime(uniform(1, 50));

                     raise(62, startNodeOp, new BasicIntSequentialDistribution(1), new IntegerUniformDistribution(1000,5000,rnd), constant(1));
                 }
             };
             SimulationScenario.StochasticProcess killLeaderNode = new SimulationScenario.StochasticProcess() {
                 {
                 	eventInterArrivalTime(constant(1000));
                     raise(1, killNodeOp, constant(64));
                 }
             };
             setup.start();
             systemSetup.start();
             
             startBootstrapServer.startAfterTerminationOf(100, systemSetup);
             startNonWriterPeers.startAfterTerminationOf(100, startBootstrapServer);
             startWriterPeers.startAfterTerminationOf(1000, startNonWriterPeers);
             // Simulation Observer for Task 2
             startObserverTask2.startAfterTerminationOf(1, startWriterPeers);
             // Simulation Observer for Task 3.1
             //startObserverTask3_1.startAfterTerminationOf(1, startWriterPeers);
             // Simulation Observer for Task 3.2
             //startObserverTask3_2.startAfterTerminationOf(1, startWriterPeers);
             // SimulationObserver for Task 4.1
             //startObserverTask4_1.startAfterTerminationOf(1, startWriterPeers);
             //killLeaderNode.startAfterTerminationOf(60000, startWriterPeers);
             terminateAfterTerminationOf(200000, setup);

         }
     };

     return scen;
 }
}
