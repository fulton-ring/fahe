"use client";

import { useState, useEffect, useCallback } from "react";
import type { UIMessage } from "@ai-sdk/react";

export interface Conversation {
  id: string;
  name: string;
  messages: UIMessage[];
  createdAt: number;
  updatedAt: number;
}

const STORAGE_KEY = "fahe-chat-conversations";

export const useChatHistory = () => {
  const [conversations, setConversations] = useState<Conversation[]>([]);
  const [currentConversationId, setCurrentConversationId] = useState<
    string | null
  >(null);
  const [isInitialized, setIsInitialized] = useState(false);

  // Load conversations from localStorage on mount
  useEffect(() => {
    const stored = localStorage.getItem(STORAGE_KEY);

    if (stored) {
      try {
        const parsed = JSON.parse(stored);
        setConversations(parsed);

        // Set the most recent conversation as current
        if (parsed.length > 0) {
          const mostRecent = parsed.reduce(
            (latest: Conversation, conv: Conversation) =>
              conv.updatedAt > latest.updatedAt ? conv : latest
          );
          setCurrentConversationId(mostRecent.id);
        }
      } catch (error) {
        console.error("Error loading chat history:", error);
      }
    }

    // Mark as initialized
    setIsInitialized(true);
  }, []);

  // Save conversations to localStorage whenever they change (but only after initialization)
  useEffect(() => {
    if (isInitialized && conversations.length > 0) {
      localStorage.setItem(STORAGE_KEY, JSON.stringify(conversations));
    }
  }, [conversations, isInitialized]);

  // Get current conversation
  const currentConversation = conversations.find(
    (conv) => conv.id === currentConversationId
  );

  // Create a new conversation
  const createConversation = useCallback((name?: string) => {
    const timestamp = Date.now();
    const newConversation: Conversation = {
      id: `conv-${timestamp}`,
      name: name || `Conversation ${Date.now()}`,
      messages: [],
      createdAt: timestamp,
      updatedAt: timestamp,
    };

    setConversations((prev) => [...prev, newConversation]);
    setCurrentConversationId(newConversation.id);
    return newConversation;
  }, []);

  // Delete a conversation
  const deleteConversation = useCallback((conversationId: string) => {
    let updatedConversations: Conversation[] = [];

    setConversations((prev) => {
      updatedConversations = prev.filter((conv) => conv.id !== conversationId);

      // If no conversations left, clear localStorage
      if (updatedConversations.length === 0) {
        localStorage.removeItem(STORAGE_KEY);
      }

      return updatedConversations;
    });

    // If deleting current conversation, switch to another
    setCurrentConversationId((prevId) => {
      if (conversationId === prevId) {
        if (updatedConversations.length > 0) {
          return updatedConversations[updatedConversations.length - 1].id;
        } else {
          return null;
        }
      }
      return prevId;
    });
  }, []);

  // Update messages in current conversation
  const updateMessages = useCallback(
    (messages: UIMessage[], conversationId?: string) => {
      const targetId = conversationId || currentConversationId;
      if (!targetId) return;

      setConversations((prev) =>
        prev.map((conv) =>
          conv.id === targetId
            ? {
                ...conv,
                messages,
                updatedAt: Date.now(),
              }
            : conv
        )
      );
    },
    [currentConversationId]
  );

  // Rename a conversation
  const renameConversation = useCallback(
    (conversationId: string, newName: string) => {
      setConversations((prev) =>
        prev.map((conv) =>
          conv.id === conversationId
            ? {
                ...conv,
                name: newName,
                updatedAt: Date.now(),
              }
            : conv
        )
      );
    },
    []
  );

  return {
    conversations,
    currentConversation,
    currentConversationId,
    setCurrentConversationId,
    createConversation,
    deleteConversation,
    updateMessages,
    renameConversation,
    isInitialized,
  };
};
