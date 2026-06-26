from __future__ import annotations

import json
import os
import re
import time
from datetime import date
from typing import Final

from logs.logger import get_logger

from agents.LlamaFour import LlamaFourClient
from agents.GptOss import GptOssClient
from agents.GptOss_safeguard import GptOss_safeguardClient
from agents.GptOss_20b import GptOss_20bClient
from agents.LlamaThree_versatile import LlamaThreeClient as LlamaThree_versatileClient
from agents.LlamaThree_instant import LlamaThree_instantClient
from agents.Groq import GroqClient

from errors.AgentBaseError import AgentBaseError
from errors.AgentRateLimitError import AgentRateLimitError
from errors.PipelineBaseError import PipelineBaseError
from config.dotenv_config import config

logger = get_logger(__name__)



_AGENT_REGISTRY: Final[list[tuple[str, type, str]]] = [
    ("Llama 4",                  LlamaFourClient,              "LLAMAFOUR_MAX_ATTEMPTS"),
    ("Llama 3",                  LlamaThree_versatileClient,   "LLAMATHREE_MAX_ATTEMPTS"),
    ("GPT OSS",                  GptOssClient,                 "GPTOSS_MAX_ATTEMPTS"),
    ("GPT OSS Safeguard",        GptOss_safeguardClient,       "GPTOSS_SAFEGUARD_MAX_ATTEMPTS"),
    ("GPT OSS 20B",              GptOss_20bClient,             "GPTOSS_20B_MAX_ATTEMPTS"),
    ("Llama 3.1 Instant",        LlamaThree_instantClient,     "LLAMATHREE_INSTANT_MAX_ATTEMPTS"),
    ("Groq",                     GroqClient,                   "GROQ_MAX_ATTEMPTS"),
]

_BASE_DIR: Final[str] = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_DEFAULT_STATE_PATH: Final[str] = os.path.join(_BASE_DIR, "Data", "failover_state.json")


class FailoverManager:
    """
    A dynamic multi-key, multi-agent failover manager designed for LLM production pipelines.

    This manager orchestrates requests across various LLM agents and API keys,
    implementing robust error handling, rate limit management, and state persistence 
    to ensure high availability and reliability.

    Features:
        - Dynamic Key Discovery: Automatically detects 'GROQ_API_KEY_N' env vars.
        - Persistent State: Tracks attempts, dropped keys, and agents in JSON to 
          survive process restarts.
        - Error Classification: Distinguishes between TPD (Tokens Per Day) which 
          requires key rotation, and RPM/TPM (Rate Limits) which requires waiting.
        - Automatic Reset: Clears usage statistics daily to manage quota limits.

    Attributes:
        _default_max (int): Global fallback for max attempts per agent/key.
        _state_path (str): File path for persistent JSON state storage.
        _keys (list[tuple[str, str]]): List of discovered (label, key_value) pairs.
        _agents (list[tuple[str, type, int]]): Registry of available agent classes.
        _state (dict): The current execution state for the day.
    """


    def __init__(self, max_attempts_per_agent: int | None = None):
        """
        Initializes the FailoverManager, loads state, and logs startup status.

        Args:
            max_attempts_per_agent (int, optional): Override for max attempts. 
                Falls back to MAX_ATTEMPTS_PER_AGENT env var or default (15).
        """

        self._default_max: int = int(
            max_attempts_per_agent
            or os.getenv("MAX_ATTEMPTS_PER_AGENT", "15")
        )
        self._state_path: str = os.getenv(
            "FAILOVER_STATE_PATH", _DEFAULT_STATE_PATH,
        )

        self._keys: list[tuple[str, str]] = self._discover_keys()
        if not self._keys:
            logger.error("No Groq API keys found in environment")

        
        self._agents: list[tuple[str, type, int]] = []
        for name, cls, env_key in _AGENT_REGISTRY:
            max_att = getattr(config, env_key, self._default_max)
            self._agents.append((name, cls, max_att))

        
        self._state: dict = self._load_or_create_state()
        self._save_state()

        
        self._log_dashboard()


    def _discover_keys(self) -> list[tuple[str, str]]:
        """
        Scan ``os.environ`` for ``GROQ_API_KEY_\\d+`` entries.

        If any numbered keys are found they are returned sorted by suffix.
        Otherwise the legacy ``GROQ_API_KEY`` / ``FAILOVER_GROQ_API_KEY``
        pair is used as a fallback.
        """
        numbered: dict[int, tuple[str, str]] = {}
        for env_name, env_value in os.environ.items():
            match = re.match(r"^GROQ_API_KEY_(\d+)$", env_name)
            if match and env_value.strip():
                numbered[int(match.group(1))] = (env_name, env_value.strip())

        if numbered:
            return [
                (label, val)
                for _, (label, val) in sorted(numbered.items())
            ]

        
        keys: list[tuple[str, str]] = []
        primary = os.getenv("GROQ_API_KEY", "").strip()
        failover = os.getenv("FAILOVER_GROQ_API_KEY", "").strip()
        if primary:
            keys.append(("GROQ_API_KEY", primary))
        if failover:
            keys.append(("FAILOVER_GROQ_API_KEY", failover))
        return keys
    

    def _load_or_create_state(self) -> dict:
        """
        Load state from disk; reset when the date field ≠ today.
        """
        today = date.today().isoformat()

        if os.path.exists(self._state_path):
            try:
                with open(self._state_path, "r", encoding="utf-8") as fh:
                    state = json.load(fh)
                if state.get("date") == today:
                    self._ensure_state_complete(state)
                    logger.info(
                        "Loaded failover state from %s (date=%s)",
                        self._state_path, today,
                    )
                    return state
                logger.info(
                    "Failover state date mismatch (%s vs %s) — resetting",
                    state.get("date"), today,
                )
            except Exception as exc:
                logger.warning(
                    "Cannot read failover state from %s: %s",
                    self._state_path, exc,
                )

        return self._fresh_state(today)

    def _fresh_state(self, today: str) -> dict:
        """Build a zeroed-out state dict for *today*."""
        return {
            "date": today,
            "agents": {
                agent_name: {
                    "dropped": False,
                    "keys": {
                        key_label: {
                            "attempts": 0,
                            "dropped": False,
                            "drop_reason": None,
                        }
                        for key_label, _ in self._keys
                    },
                }
                for agent_name, _, _ in self._agents
            },
        }

    def _ensure_state_complete(self, state: dict):
        """
        Ensure all registered agents and keys are present in the loaded state.
        If an agent was dropped but new keys are discovered, undrop the agent.
        """
        if "agents" not in state:
            state["agents"] = {}

        for agent_name, _, _ in self._agents:
            if agent_name not in state["agents"]:
                state["agents"][agent_name] = {"dropped": False, "keys": {}}

            for key_label, _ in self._keys:
                if key_label not in state["agents"][agent_name]["keys"]:
                    state["agents"][agent_name]["keys"][key_label] = {
                        "attempts": 0,
                        "dropped": False,
                        "drop_reason": None,
                    }
                    
    
            all_keys_dropped = all(
                k.get("dropped", False) 
                for k in state["agents"][agent_name]["keys"].values()
            )
            state["agents"][agent_name]["dropped"] = all_keys_dropped

    def _save_state(self) -> None:
        """Atomically persist state (write-tmp → ``os.replace``)."""
        try:
            os.makedirs(os.path.dirname(self._state_path), exist_ok=True)
            tmp = self._state_path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(self._state, fh, indent=2, ensure_ascii=False)
            os.replace(tmp, self._state_path)
        except Exception as exc:
            logger.warning(
                "Failed to persist failover state to %s: %s",
                self._state_path, exc,
            )
  

    def _key_state(self, agent_name: str, key_label: str) -> dict:
        return (
            self._state
            .get("agents", {})
            .get(agent_name, {})
            .get("keys", {})
            .get(key_label, {})
        )

    def _is_agent_dropped(self, agent_name: str) -> bool:
        return self._state.get("agents", {}).get(
            agent_name, {}
        ).get("dropped", False)

    def _is_key_dropped(self, agent_name: str, key_label: str) -> bool:
        return self._key_state(agent_name, key_label).get("dropped", False)

    def _get_attempts(self, agent_name: str, key_label: str) -> int:
        return self._key_state(agent_name, key_label).get("attempts", 0)

    def _increment_attempts(self, agent: str, key: str) -> int:
        """Bump counter (in-memory only) and return the new value.

        State is NOT written to disk on every increment to avoid thousands of
        disk writes per run.  It is persisted on key-drop and agent-drop events,
        which are the only outcomes that need to survive a crash.
        """
        ks = self._state["agents"][agent]["keys"][key]
        ks["attempts"] += 1
        return ks["attempts"]

    def _reset_attempts(self, agent: str, key: str) -> None:
        self._state["agents"][agent]["keys"][key]["attempts"] = 0

    def _drop_key(self, agent: str, key: str, reason: str) -> None:
        ks = self._state["agents"][agent]["keys"][key]
        ks["dropped"] = True
        ks["drop_reason"] = reason
        self._save_state()
        logger.warning(
            "[%s/%s] Key DROPPED (reason=%s, attempts=%d)",
            agent, key, reason, ks["attempts"],
        )

    def _drop_agent(self, agent: str) -> None:
        self._state["agents"][agent]["dropped"] = True
        self._save_state()
        logger.warning("[%s] Agent DROPPED — all keys exhausted", agent)

    def _all_keys_dropped(self, agent: str) -> bool:
        keys = self._state.get("agents", {}).get(agent, {}).get("keys", {})
        return bool(keys) and all(
            v.get("dropped", False) for v in keys.values()
        )

    def _all_agents_dropped(self) -> bool:
        agents = self._state.get("agents", {})
        return bool(agents) and all(
            v.get("dropped", False) for v in agents.values()
        )

    def _log_dashboard(self) -> None:
        """Emit a structured table at startup showing agent × key status."""
        today = self._state.get("date", "unknown")

        lines = [
            "",
            "=" * 70,
            f"  SPARK LLM FAILOVER STATUS — {today}",
            "=" * 70,
            f"  {'Agent':<10} | {'Key':<20} | {'Attempts':>10} | Status",
            "-" * 70,
        ]

        for agent_name, _, max_att in self._agents:
            agent_st = self._state.get("agents", {}).get(agent_name, {})
            for key_label, _ in self._keys:
                ks = agent_st.get("keys", {}).get(key_label, {})
                att = ks.get("attempts", 0)
                dropped = ks.get("dropped", False)
                reason = ks.get("drop_reason", "")

                status = (
                    f"DROPPED({reason})" if dropped else "ACTIVE"
                )
                lines.append(
                    f"  {agent_name:<10} | {key_label:<20} | "
                    f"{att:>4}/{max_att:<4}  | {status}"
                )

        lines.append("=" * 70)
        logger.info("\n".join(lines))


    def predict_missing_value(self, prompt: str) -> str | None:
        """
        Executes the failover logic to generate a response from an LLM.

        The rotation hierarchy is:
            1. Iterate through Agents (Outer)
            2. Iterate through API Keys (Middle)
            3. Retry logic per Key (Inner)

        Error Handling Policy:
            - Success: Resets attempt counters for that key.
            - TPD Error: Immediately drops the key (daily limit reached).
            - Rate Limits (RPM/TPM): Sleeps for 'retry_after' duration.
            - Validation Errors: Drops the entire agent due to persistent failure.

        Args:
            prompt (str): The input text to be processed by the LLMs.

        Returns:
            str | None: The extracted value if successful, None if no model 
                succeeds after exhaustive retries.

        Raises:
            PipelineBaseError: If all registered agents and keys are exhausted 
                for the current day.
        """
        
        for agent_name, AgentClass, max_attempts in self._agents:
            if self._is_agent_dropped(agent_name):
                continue

            for key_label, key_value in self._keys:
                if self._is_key_dropped(agent_name, key_label):
                    continue

                agent = AgentClass(api_key=key_value)

            
                while True:
                    current = self._get_attempts(agent_name, key_label)
                    if current >= max_attempts:
                        self._drop_key(agent_name, key_label, "max_attempts")
                        break 

                    try:
                        result = agent.generate_text(prompt=prompt)

                        if result and str(result).strip():
                            logger.info(
                                "[%s/%s] Success on attempt %d",
                                agent_name, key_label, current + 1,
                            )
                            self._reset_attempts(agent_name, key_label)
                            return str(result).strip()

                    
                        logger.warning(
                            "[%s/%s] Empty response (attempt %d/%d)",
                            agent_name, key_label,
                            current + 1, max_attempts,
                        )
                        new = self._increment_attempts(agent_name, key_label)
                        if new >= max_attempts:
                            self._drop_key(agent_name, key_label, "empty")
                        break  

                    except AgentRateLimitError as e:
                        new = self._increment_attempts(agent_name, key_label)

                        if e.limit_type == "tpd":
                            logger.warning(
                                "[%s/%s] TPD limit — dropping key immediately",
                                agent_name, key_label,
                            )
                            self._drop_key(agent_name, key_label, "tpd")
                            break  

                        
                        logger.warning(
                            "[%s/%s] %s limit (attempt %d/%d), "
                            "sleeping %.1fs",
                            agent_name, key_label,
                            e.limit_type.upper(),
                            new, max_attempts,
                            e.retry_after,
                        )
                        if e.retry_after > 0:
                            time.sleep(e.retry_after)

                        if new >= max_attempts:
                            self._drop_key(agent_name, key_label, e.limit_type)
                            break  

                    except AgentBaseError as e:
                        err_msg = getattr(e, "message", str(e))
                        if "json_validate_failed" in err_msg:
                            logger.warning(
                                "[%s] JSON validation failed — dropping entire agent, "
                                "skipping remaining keys",
                                agent_name,
                            )
                            self._drop_agent(agent_name)
                            break  
                        logger.warning(
                            "[%s/%s] Agent error (attempt %d/%d): %s",
                            agent_name, key_label,
                            current + 1, max_attempts,
                            err_msg,
                        )
                        new = self._increment_attempts(agent_name, key_label)
                        if new >= max_attempts:
                            self._drop_key(
                                agent_name, key_label, "agent_error",
                            )
                        break  

                    except Exception as e:
                        logger.error(
                            "[%s/%s] Unexpected error (attempt %d/%d): %s",
                            agent_name, key_label,
                            current + 1, max_attempts, e,
                            exc_info=True,
                        )
                        new = self._increment_attempts(agent_name, key_label)
                        if new >= max_attempts:
                            self._drop_key(
                                agent_name, key_label, "unexpected",
                            )
                        break 

            
            if self._all_keys_dropped(agent_name):
                self._drop_agent(agent_name)

        
        if self._all_agents_dropped():
            raise PipelineBaseError(
                "LLM Failover",
                "All LLM agents and API keys exhausted for today. "
                "Pipeline cannot continue without LLM imputation.",
            )

        logger.error(
            "All active agents returned empty / failed for this prompt"
        )
        return None
