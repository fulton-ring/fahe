"use client";

import { Button } from "@/components/ui/button";
import { XIcon, PlusIcon, Trash2Icon } from "lucide-react";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
  AlertDialogTrigger,
} from "@/components/ui/alert-dialog";
import type { Conversation } from "@/hooks/useChatHistory";

interface ChatHeaderProps {
  conversations: Conversation[];
  currentConversationId: string | null;
  onConversationChange: (id: string) => void;
  onNewConversation: () => void;
  onDeleteConversation: () => void;
  onClose: () => void;
}

export const ChatHeader = ({
  conversations,
  currentConversationId,
  onConversationChange,
  onNewConversation,
  onDeleteConversation,
  onClose,
}: ChatHeaderProps) => {
  return (
    <div className="flex flex-col gap-3 p-4 border-b bg-white flex-none">
      <div className="flex items-center justify-between">
        <h2 className="text-lg font-semibold">Map Assistant</h2>
        <Button
          variant="ghost"
          size="icon"
          onClick={onClose}
          className="shrink-0"
        >
          <XIcon className="h-5 w-5" />
        </Button>
      </div>

      {/* Conversation Management */}
      <div className="flex flex-col gap-2">
        <div className="flex gap-2">
          <Select
            value={currentConversationId || ""}
            onValueChange={onConversationChange}
          >
            <SelectTrigger className="flex-1 h-8 text-xs">
              <SelectValue placeholder="Select conversation" />
            </SelectTrigger>
            <SelectContent>
              {conversations.map((conv) => (
                <SelectItem key={conv.id} value={conv.id} className="text-xs">
                  {conv.name}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          <Button
            variant="outline"
            size="sm"
            onClick={onNewConversation}
            className="h-8 px-2"
            title="New conversation"
          >
            <PlusIcon className="h-4 w-4" />
          </Button>
          <AlertDialog>
            <AlertDialogTrigger asChild>
              <Button
                variant="outline"
                size="sm"
                className="h-8 px-2"
                title="Delete conversation"
                disabled={conversations.length === 0}
              >
                <Trash2Icon className="h-4 w-4" />
              </Button>
            </AlertDialogTrigger>
            <AlertDialogContent>
              <AlertDialogHeader>
                <AlertDialogTitle>Delete conversation?</AlertDialogTitle>
                <AlertDialogDescription>
                  This action cannot be undone. This will permanently delete
                  this conversation and all its messages.
                </AlertDialogDescription>
              </AlertDialogHeader>
              <AlertDialogFooter>
                <AlertDialogCancel>Cancel</AlertDialogCancel>
                <AlertDialogAction onClick={onDeleteConversation}>
                  Delete
                </AlertDialogAction>
              </AlertDialogFooter>
            </AlertDialogContent>
          </AlertDialog>
        </div>
      </div>
    </div>
  );
};
