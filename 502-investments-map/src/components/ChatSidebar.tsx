"use client";

import { useChat } from "@ai-sdk/react";
import { useState, useRef, useEffect } from "react";
import { MessageSquareIcon } from "lucide-react";
import { Message, MessageContent } from "@/components/ai-elements/message";
import {
  PromptInput,
  PromptInputBody,
  PromptInputFooter,
  PromptInputSubmit,
  PromptInputTextarea,
  type PromptInputMessage,
} from "@/components/ai-elements/prompt-input";
import {
  Conversation,
  ConversationContent,
  ConversationEmptyState,
  ConversationScrollButton,
} from "@/components/ai-elements/conversation";
import { Loader } from "@/components/ai-elements/loader";
import {
  Reasoning,
  ReasoningContent,
  ReasoningTrigger,
} from "@/components/ai-elements/reasoning";
import { ChatHeader } from "@/components/chat/ChatHeader";
import { ToolPart } from "@/components/chat/ToolPart";
import { useChatHistory } from "@/hooks/useChatHistory";
import type { Geometry } from "geojson";

interface ChatSidebarProps {
  onClose: () => void;
  onFlyTo?: (geometry: Geometry) => void;
}

// Component to render text message parts
const MessageTextPart = ({ text }: { text: string }) => {
  if (!text) return null;

  return <div className="text-sm whitespace-pre-wrap">{text}</div>;
};

const ChatSidebar = ({ onClose, onFlyTo }: ChatSidebarProps) => {
  const [input, setInput] = useState("");
  const isLoadingMessages = useRef(false);

  const {
    conversations,
    currentConversation,
    currentConversationId,
    setCurrentConversationId,
    createConversation,
    deleteConversation,
    updateMessages,
    isInitialized,
  } = useChatHistory();

  // Initialize with a conversation if none exists (but wait for initial load)
  useEffect(() => {
    // Only create a conversation if we've initialized and truly have none
    if (isInitialized && conversations.length === 0 && !currentConversationId) {
      createConversation("New Conversation");
    }
  }, [
    conversations.length,
    currentConversationId,
    isInitialized,
    createConversation,
  ]);

  const { messages, sendMessage, status, error, setMessages } = useChat({
    id: currentConversationId || undefined,
    onError: (error) => {
      console.error("Chat error:", error);
      // Error will be displayed in the UI via the error state
    },
    onFinish: (message) => {
      // Save after a small delay to ensure messages state is updated
      setTimeout(() => {
        if (currentConversationId) {
          // Get the latest messages from the chat hook
          setMessages((currentMessages) => {
            updateMessages(currentMessages);
            return currentMessages;
          });
        }
      }, 100);
    },
  });

  // Load messages when switching conversations or on mount
  useEffect(() => {
    if (currentConversation) {
      isLoadingMessages.current = true;
      setMessages(currentConversation.messages);
      // Reset flag after a short delay to allow React to process
      setTimeout(() => {
        isLoadingMessages.current = false;
      }, 100);
    }
  }, [currentConversationId, currentConversation, setMessages]);

  // Display errors as chat messages
  const lastErrorRef = useRef<Error | null>(null);

  useEffect(() => {
    // Only show error once (prevent duplicates)
    if (error && error !== lastErrorRef.current) {
      console.error("Chat error occurred:", error);
      lastErrorRef.current = error;

      // Add error message to chat
      setMessages((prev) => [
        ...prev,
        {
          id: `error-${Date.now()}`,
          role: "assistant",
          parts: [
            {
              type: "text",
              text: `⚠️ Error: ${
                error.message ||
                "An error occurred while processing your message. Please try again."
              }`,
            },
          ],
        },
      ]);
    }
  }, [error, setMessages]);

  const isLoading = status === "submitted" || status === "streaming";

  const handleSubmit = (message: PromptInputMessage) => {
    if (!message.text?.trim() || isLoading) return;

    sendMessage({ text: message.text });
    setInput("");
  };

  const handleNewConversation = () => {
    createConversation();
  };

  const handleDeleteConversation = () => {
    if (currentConversationId) {
      deleteConversation(currentConversationId);
    }
  };

  return (
    <div className="h-full bg-white md:border-l flex flex-col w-full overflow-hidden">
      {/* Header */}
      <ChatHeader
        conversations={conversations}
        currentConversationId={currentConversationId}
        onConversationChange={setCurrentConversationId}
        onNewConversation={handleNewConversation}
        onDeleteConversation={handleDeleteConversation}
        onClose={onClose}
      />

      {/* Messages Container */}
      <Conversation>
        <ConversationContent className="space-y-4">
          {messages.length === 0 ? (
            <ConversationEmptyState
              icon={<MessageSquareIcon className="size-12" />}
              title="Map Assistant"
              description="Hi! 👋 I can help you understand the investment and economic data on this map. What would you like to know?"
            />
          ) : (
            <>
              {messages.map((message) => {
                const isLastMessage = message.id === messages.at(-1)?.id;
                const isCurrentlyStreaming =
                  status === "streaming" && isLastMessage;

                return (
                  <Message key={message.id} from={message.role}>
                    <MessageContent>
                      {message.parts.map((part, i) => {
                        // Handle text parts
                        if (part.type === "text") {
                          return (
                            <MessageTextPart
                              key={`${message.id}-${i}`}
                              text={part.text}
                            />
                          );
                        }

                        // Handle reasoning parts - only show if currently streaming
                        if (part.type === "reasoning") {
                          // Skip reasoning for completed messages
                          if (!isCurrentlyStreaming) {
                            return null;
                          }

                          const isLastPart = i === message.parts.length - 1;
                          const isStreamingThisPart =
                            isLastPart && isCurrentlyStreaming;

                          return (
                            <Reasoning
                              key={`${message.id}-${i}`}
                              className="w-full"
                              isStreaming={isStreamingThisPart}
                            >
                              <ReasoningTrigger />
                              <ReasoningContent>{part.text}</ReasoningContent>
                            </Reasoning>
                          );
                        }

                        // Handle tool-related parts (dynamic tools)
                        if (part.type === "dynamic-tool") {
                          return (
                            <ToolPart
                              key={`${message.id}-${i}`}
                              part={part}
                              onFlyTo={onFlyTo}
                            />
                          );
                        }

                        // Default: render nothing for unknown part types
                        return null;
                      })}
                    </MessageContent>
                  </Message>
                );
              })}

              {status === "submitted" && <Loader />}
            </>
          )}
        </ConversationContent>
        <ConversationScrollButton />
      </Conversation>

      {/* Input Form */}
      <div className="bg-white p-4 flex-none">
        <PromptInput onSubmit={handleSubmit}>
          <PromptInputBody>
            <PromptInputTextarea
              value={input}
              onChange={(e) => setInput(e.target.value)}
              placeholder="Ask about the data..."
              disabled={isLoading}
              className="text-sm"
            />
          </PromptInputBody>
          <PromptInputFooter className="justify-end">
            <PromptInputSubmit
              disabled={!input.trim() || isLoading}
              status={status}
            />
          </PromptInputFooter>
        </PromptInput>
      </div>
    </div>
  );
};

export default ChatSidebar;
