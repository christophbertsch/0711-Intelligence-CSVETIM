"""
Main entry point for CSV Import Guardian agents.
"""

import asyncio
import logging
import os
import sys
from typing import List

from shared.base_agent import AgentRegistry, run_agent, run_multiple_agents
from shared.config import get_log_config

# Import all agents to register them
from agents.ingest_agent import IngestAgent
from agents.profiling_agent import ProfilingAgent
from agents.mapping_agent import MappingAgent
from agents.validation_agent import ValidationAgent
from agents.normalization_agent import NormalizationAgent
from agents.persistence_agent import PersistenceAgent
from agents.export_agent import ExportAgent
from agents.dq_reporter_agent import DQReporterAgent
from agents.lineage_agent import LineageAgent

# Configure logging
logging.config.dictConfig(get_log_config())
logger = logging.getLogger(__name__)


def main():
    """Main entry point for running agents."""
    # Get agent name from environment variable
    agent_name = os.getenv("AGENT_NAME")
    
    if not agent_name:
        logger.error("AGENT_NAME environment variable not set")
        sys.exit(1)
    
    # Check if agent is registered
    try:
        AgentRegistry.get_agent_class(agent_name)
    except ValueError as e:
        logger.error(f"Unknown agent: {agent_name}")
        logger.info(f"Available agents: {AgentRegistry.list_agents()}")
        sys.exit(1)
    
    # Run the agent
    logger.info(f"Starting agent: {agent_name}")
    
    try:
        asyncio.run(run_agent(agent_name))
    except KeyboardInterrupt:
        logger.info("Agent stopped by user")
    except Exception as e:
        logger.error(f"Agent failed: {e}")
        sys.exit(1)


def run_all_agents():
    """Run all registered agents concurrently."""
    agent_names = AgentRegistry.list_agents()
    
    if not agent_names:
        logger.error("No agents registered")
        sys.exit(1)
    
    logger.info(f"Starting all agents: {agent_names}")
    
    try:
        asyncio.run(run_multiple_agents(agent_names))
    except KeyboardInterrupt:
        logger.info("All agents stopped by user")
    except Exception as e:
        logger.error(f"Error running agents: {e}")
        sys.exit(1)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "all":
        run_all_agents()
    else:
        main()