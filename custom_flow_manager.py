"""
Custom FlowManager with context strategy that summarizes conversation history
before node transitions.
"""

from typing import List, Optional

from pipecat.frames.frames import LLMMessagesUpdateFrame, LLMSetToolsFrame
from pipecat_flows import ContextStrategyConfig, FlowError, FlowManager

from loguru import logger as log

class CustomFlowManager(FlowManager):
    """
    Custom FlowManager with custom context strategy that summarizes conversation history
    before node transitions.
    """

    def _summarize_conversation_history(self, messages: list) -> str:
        """
        Summarize conversation history (excluding system messages).

        Args:
            messages: List of message dictionaries or Pydantic models with 'role' and 'content' keys/attributes

        Returns:
            Only the conversation without system messages
        """
        # Helper to extract role and content from either dict or Pydantic model
        def get_role_content(msg):
            if isinstance(msg, dict):
                return msg.get("role"), msg.get("content", "")
            else:
                # Handle Pydantic model with attributes
                return getattr(msg, "role", None), getattr(msg, "content", "")

        # Filter out system messages and get only user/assistant messages
        conversation_messages = []
        for msg in messages:
            role, _ = get_role_content(msg)
            if role in ["user", "assistant"]:
                conversation_messages.append(msg)

        if not conversation_messages:
            return ""

        # Create a simple summary by joining key points
        summary_parts = []
        for msg in conversation_messages:
            role, content = get_role_content(msg)
            role_label = "Customer" if role == "user" else "Assistant"
            content_str = str(content).strip() if content else ""
            if content_str:
                summary_parts.append(f"{role_label}: {content_str}")

        # Join into single line
        summary = " | ".join(summary_parts)
        return summary

    async def _update_llm_context(
        self,
        role_messages: Optional[List[dict]],
        task_messages: List[dict],
        functions: List[dict],
        strategy: Optional[ContextStrategyConfig] = None,
    ) -> None:
        """Update LLM context with new messages and functions using custom strategy.

        Args:
            role_messages: Optional role messages to add to context.
            task_messages: Task messages to add to context.
            functions: New functions to make available.
            strategy: Optional context update configuration.

        Raises:
            FlowError: If context update fails.
        """
        try:
            messages = []

            if role_messages:
                messages.extend(role_messages)

            conversation_summary = ""
            if (
                self._context_aggregator
                and self._context_aggregator.user()._context
            ):
                # Get current context messages
                current_context_messages = []
                try:
                    user_agg = self._context_aggregator.user()
                    if hasattr(user_agg, '_context') and hasattr(user_agg._context, 'messages'):
                        current_context_messages = user_agg._context.messages
                except Exception as e:
                    log.warning(f"Error accessing context messages: {e}")

                if current_context_messages:
                    conversation_summary = self._summarize_conversation_history(current_context_messages)
                    if len(conversation_summary) > 600:
                        conversation_summary = "..." + conversation_summary[-150:]
                    if conversation_summary:
                        messages.append({
                            "role": "user",
                            "content": conversation_summary
                        })

            # Add task messages
            messages.extend(task_messages)

            await self._task.queue_frames(
                [LLMMessagesUpdateFrame(messages=messages), LLMSetToolsFrame(tools=functions)]
            )

            log.debug(
                f"Updated LLM context using LLMMessagesUpdateFrame with custom strategy "
                f"(summary: {len(conversation_summary)} chars, role_msgs: {len(role_messages) if role_messages else 0}, task_msgs: {len(task_messages)})"
            )

        except Exception as e:
            log.error(f"Failed to update LLM context: {str(e)}")
            raise FlowError(f"Context update failed: {str(e)}") from e
