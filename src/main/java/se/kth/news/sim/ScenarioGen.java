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
package se.kth.news.sim;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import se.kth.news.sim.compatibility.SimNodeIdExtractor;
import se.kth.news.system.HostMngrComp;
import se.sics.kompics.Init;
import se.sics.kompics.network.Address;
import se.sics.kompics.simulator.SimulationScenario;
import se.sics.kompics.simulator.adaptor.Operation;
import se.sics.kompics.simulator.adaptor.Operation1;
import se.sics.kompics.simulator.adaptor.Operation2;
import se.sics.kompics.simulator.adaptor.distributions.ConstantDistribution;
import se.sics.kompics.simulator.adaptor.distributions.IntegerUniformDistribution;
import se.sics.kompics.simulator.adaptor.distributions.extra.BasicIntSequentialDistribution;
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
	                		gv.setValue("simulation.newsstore", new GlobalNewsStore());

	                }
	            };
	        }
	    };
	    
	    static Operation1 startObserverOp = new Operation1<StartNodeEvent, Long>() {
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
	                    return SimulationObserverTask1.class;
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

    static Operation2<StartNodeEvent, Integer, Integer> startNodeOp = new Operation2<StartNodeEvent, Integer, Integer>() {

        @Override
        public StartNodeEvent generate(final Integer nodeId, final Integer timer) {
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
                    int test = 5000000;
                    nodeConfig.put("newsTimeOut", test);
                    return nodeConfig;
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
                
                SimulationScenario.StochasticProcess startObserver = new SimulationScenario.StochasticProcess() {
                    {
                    	eventInterArrivalTime(constant(1000));
                        raise(1, startObserverOp, constant(0));
                    }
                };
                StochasticProcess startPeers = new StochasticProcess() {
                    {
                    	
                        eventInterArrivalTime(uniform(1000, 1100));
                        raise(20, startNodeOp, new BasicIntSequentialDistribution(1), new IntegerUniformDistribution(50000,100000,rnd));
                    }
                };
                setup.start();
                systemSetup.start();
                
                startBootstrapServer.startAfterTerminationOf(1000, systemSetup);
                startPeers.startAfterTerminationOf(1000, startBootstrapServer);
                startObserver.startAfterTerminationOf(1, startPeers);
                terminateAfterTerminationOf(10000000, startPeers);
            }
        };

        return scen;
    }
}
