import re
import time
import traceback
import inspect
from functools import wraps

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.utils import AnalysisException

from logs.logger import get_logger

from errors.AgentRateLimitError import AgentRateLimitError
from errors.ConfigurationError  import ConfigurationError
from errors.ComputationError    import ComputationError
from errors.DataQualityError    import DataQualityError
from errors.PipelineBaseError   import PipelineBaseError
from errors.SchemaMismatchError import SchemaMismatchError

logger = get_logger(__name__)


_RATE_LIMIT_KEYWORDS = [
    "rate limit", "429", "quota", "too many requests", "capacity",
]

_TPD_PATTERNS = re.compile(
    r"tokens?\s*per\s*day|tpd|daily.?limit|daily.?quota", re.IGNORECASE,
)
_TPM_PATTERNS = re.compile(
    r"tokens?\s*per\s*minute|tpm", re.IGNORECASE,
)

_RETRY_AFTER_RE = re.compile(
    r"(?:try\s+again\s+in|retry[\s\-]*after)\s+"
    r"(?:(\d+)\s*m\s*)?"        # optional minutes
    r"(\d+(?:\.\d+)?)\s*s",     # required seconds
    re.IGNORECASE,
)


def _classify_limit_type(error_msg: str) -> str:
    """
    Inspect the 429 error message to determine if it is a
    tokens-per-day, tokens-per-minute, or requests-per-minute limit.
    """
    if _TPD_PATTERNS.search(error_msg):
        return "tpd"
    if _TPM_PATTERNS.search(error_msg):
        return "tpm"
    return "rpm"


def _parse_retry_after(exc: Exception, error_msg: str) -> float:
    """
    Extract retry-after seconds from either:
      1. The Groq SDK ``response.headers['retry-after']``.
      2. A regex match on the error message text.

    Returns 0.0 when the value cannot be determined.
    """
    try:
        headers = getattr(getattr(exc, "response", None), "headers", None)
        if headers is not None:
            raw = headers.get("retry-after")
            if raw is not None:
                return float(raw)
    except (ValueError, TypeError, AttributeError):
        pass

    match = _RETRY_AFTER_RE.search(error_msg)
    if match:
        minutes = int(match.group(1) or 0)
        seconds = float(match.group(2))
        return minutes * 60 + seconds

    return 0.0

def safe_step(step_name: str):
    """
    Universal Decorator to wrap PySpark transformations with robust error handling.
        Automatically translates raw Spark errors into custom categorical errors.

    Args:
        step_name (str): A descriptive name for the transformation step, used in error messages.
    Returns:
        A decorator that can be applied to both simple transformation functions and closure-based transformers.
    """

    def decorator(func):

        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                call_args = inspect.signature(func).bind_partial(*args, **kwargs).arguments
                filtered_args = {k: v for k, v in call_args.items() if not isinstance(v, SparkDataFrame)}
                if filtered_args:
                    logger.debug(f"[{step_name}] Initializing with args: {filtered_args}")
            except Exception:
                pass

            logger.info(f"[{step_name}] Step execution started...")
            start_time = time.time()

            try:
                result = func(*args, **kwargs)
            except PipelineBaseError:
                raise
            except AnalysisException as e:
                logger.error(f"[{step_name}] Spark AnalysisException: {e}", exc_info=True)
                raise SchemaMismatchError(step_name, str(e)) from None
            except Exception as e:
                logger.error(f"[{step_name}] Unexpected error: {e}", exc_info=True)
                raise ComputationError(step_name, str(e)) from None

            if isinstance(result, SparkDataFrame):
                execution_time = time.time() - start_time
                logger.info(f"[{step_name}] Step completed successfully in {execution_time:.2f} seconds.")
                return result

            elif callable(result):
                transform_func = result
                
                @wraps(transform_func)
                def inner_wrapper(df: SparkDataFrame):
                    logger.info(f"[{step_name}] Applying transformation closure...")
                    closure_start_time = time.time()
                    
                    try:
                        transformed_df = transform_func(df)
                        closure_execution_time = time.time() - closure_start_time

                        row_msg = ""
                        try:
                            from config.pipeline_config import LOG_ROW_COUNTS
                            if LOG_ROW_COUNTS:
                                row_count = transformed_df.count()
                                row_msg = f" | rows={row_count}"
                        except ImportError:
                            pass

                        logger.info(
                            f"[{step_name}] Transformation DAG node added successfully "
                            f"in {closure_execution_time:.2f}s{row_msg}."
                        )
                        return transformed_df
                        
                    except PipelineBaseError:
                        raise
                    except AnalysisException as e:
                        logger.error(
                            f"[{step_name}] Spark AnalysisException during transform: {e}\n"
                            f"{traceback.format_exc()}"
                        )
                        raise SchemaMismatchError(step_name, str(e)) from None
                    except Exception as e:
                        logger.error(
                            f"[{step_name}] Unexpected error during transform: {e}\n"
                            f"{traceback.format_exc()}"
                        )
                        raise ComputationError(step_name, str(e)) from None

                return inner_wrapper

            else:
                error_msg = f"Expected step to return a SparkDataFrame or callable, got {type(result).__name__}"
                logger.error(f"[{step_name}] Type Error: {error_msg}")
                raise ComputationError(step_name, error_msg)

        return wrapper
    return decorator



def handle_agent_errors(api_error_class):
    """
    Decorator for LLM agent methods to catch and categorize exceptions into custom error classes.

    Args:
        api_error_class: The specific custom error class to raise for API-related exceptions (e.g., GroqAPIError, DeepseekAPIError, GeminiAPIError).
    Returns:
        A decorator that wraps the target function with enhanced error handling and logging.
    """

    def decorator(func):

        @wraps(func)
        def wrapper(self, *args, **kwargs): 
            
            agent_name = getattr(self, "name", self.__class__.__name__)

            try:
                call_args = inspect.signature(func).bind_partial(self, *args, **kwargs).arguments
                call_args.pop('self', None) 
                
                if call_args:
                    payload_preview = str(call_args)[:200]
                    if len(str(call_args)) > 200:
                        payload_preview += "... [truncated]"
                    logger.debug(f"[{agent_name}] Calling '{func.__name__}' with payload preview: {payload_preview}")
            except Exception:
                pass 

            logger.info(f"[{agent_name}] Starting API request...")
            start_time = time.time()
            
            try:
                result = func(self, *args, **kwargs)
                
                execution_time = time.time() - start_time
                logger.info(f"[{agent_name}] Request completed successfully in {execution_time:.2f} seconds.")
                    
                return result
                
            except Exception as e:
                execution_time = time.time() - start_time
                error_msg = str(e).lower()
                
                logger.error(
                    f"[{agent_name}] Failed after {execution_time:.2f} seconds. Error: {e}", 
                    exc_info=True 
                )
                
                if any(keyword in error_msg for keyword in _RATE_LIMIT_KEYWORDS):
                    limit_type = _classify_limit_type(error_msg)
                    retry_after = _parse_retry_after(e, error_msg)

                    logger.warning(
                        "[%s] Rate limit classified as %s (retry_after=%.1fs)",
                        agent_name, limit_type.upper(), retry_after,
                    )

                    raise AgentRateLimitError(
                        f"[{agent_name}] Rate limit exceeded: {e}",
                        limit_type=limit_type,
                        retry_after=retry_after,
                    ) from None
                
                raise api_error_class(f"[{agent_name}] API request failed: {e}") from None
                
        return wrapper
    return decorator