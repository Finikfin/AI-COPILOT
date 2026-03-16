import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { Bot, RotateCcw, Send, Sparkles, User, X } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { ScrollArea } from '@/components/ui/scroll-area';
import { Badge } from '@/components/ui/badge';
import { cn, generateUUID } from '@/lib/utils';
import {
  generatePipeline,
  getPipelineDialogHistory,
  listPipelineDialogs,
  type GeneratePipelineResponse,
} from '@/api/chat';
import { usePipelineContext } from '@/contexts/PipelineContext';
import { useAuth } from '@/contexts/AuthContext';
import { useQueryClient } from '@tanstack/react-query';

interface Message {
  role: 'user' | 'assistant';
  content: string;
  isGenerating?: boolean;
}

interface SynthesisChatProps {
  onSynthesize?: (prompt: string) => void;
  onClose?: () => void;
  className?: string;
  initialMessage?: string;
  initialDialogId?: string;
}

const DEFAULT_ASSISTANT_MESSAGE =
  'Привет! Я помогу собрать Pipeline. Опишите бизнес-задачу, которую хотите автоматизировать.';
const ASSISTANT_THINKING_MESSAGE =
  'Анализирую возможности... Подбираю нужные Capabilities.';

const isPipelineReady = (payload: GeneratePipelineResponse | null | undefined) => {
  if (!payload) {
    return false;
  }
  return (
    (payload.status === 'ready' || payload.status === 'success') &&
    Array.isArray(payload.nodes) &&
    payload.nodes.length > 0
  );
};

const buildDialogStorageKey = (userId: string | undefined) =>
  userId ? `pipeline_active_dialog_id:${userId}` : 'pipeline_active_dialog_id:anonymous';

export const SynthesisChat: React.FC<SynthesisChatProps> = ({
  onSynthesize,
  onClose,
  className,
  initialMessage,
  initialDialogId,
}) => {
  const { setPipeline, setIsHydrating: setContextHydrating } = usePipelineContext();
  const { user } = useAuth();
  const queryClient = useQueryClient();
  const [messages, setMessages] = useState<Message[]>([
    {
      role: 'assistant',
      content: DEFAULT_ASSISTANT_MESSAGE,
    },
  ]);
  const [inputValue, setInputValue] = useState('');
  const [dialogId, setDialogId] = useState<string | null>(initialDialogId || null);
  const [isTyping, setIsTyping] = useState(false);
  const [isHydrating, setIsHydrating] = useState(true);
  const scrollRef = useRef<HTMLDivElement>(null);
  const initialMessageProcessed = useRef(false);
  const storageKey = useMemo(() => buildDialogStorageKey(user?.id), [user?.id]);

  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [messages]);

  useEffect(() => {
    initialMessageProcessed.current = false;
  }, [initialDialogId, initialMessage]);

  useEffect(() => {
    let cancelled = false;

    const hydrateDialog = async () => {
      setIsHydrating(true);
      setContextHydrating(true);
      let activeDialogId: string | null = null;
      let shouldLoadHistory = false;
      const storedDialogId = localStorage.getItem(storageKey);
      let dialogs: Array<{ dialog_id: string }> = [];
      let dialogsLoaded = false;

      try {
        dialogs = await listPipelineDialogs(50, 0);
        dialogsLoaded = true;
      } catch (error) {
        console.error('Unable to load dialogs list:', error);
      }

      if (initialDialogId) {
        activeDialogId = initialDialogId;
        // Even if initialMessage is present, someone might have refreshed the page.
        // We should try to load history first to see if the message was already sent.
        shouldLoadHistory = true;
      } else if (storedDialogId) {
        if (!dialogsLoaded || dialogs.some((dialog) => dialog.dialog_id === storedDialogId)) {
          activeDialogId = storedDialogId;
          shouldLoadHistory = true;
        }
      } else if (dialogs.length > 0) {
        activeDialogId = dialogs[0].dialog_id;
        shouldLoadHistory = true;
      } else {
        activeDialogId = generateUUID();
      }

      if (cancelled) {
        return;
      }

      setDialogId(activeDialogId);
      localStorage.setItem(storageKey, activeDialogId);

      if (!shouldLoadHistory) {
        setMessages([{ role: 'assistant', content: DEFAULT_ASSISTANT_MESSAGE }]);
        // Don't reset pipeline immediately if we are switching to a new dialog, 
        // but only if it's truly a fresh state
        if (!initialMessage) {
          setPipeline(null);
        }
        setIsHydrating(false);
        setContextHydrating(false);
        return;
      }

      try {
        const history = await getPipelineDialogHistory(activeDialogId, 30, 0);
        if (cancelled) {
          return;
        }

        if (history.messages.length > 0) {
          setMessages(
            history.messages.map((message) => ({
              role: message.role,
              content: message.content,
            }))
          );

          const latestAssistantWithPayload = [...history.messages]
            .reverse()
            .find((message) => message.role === 'assistant' && message.assistant_payload);
          const payload = latestAssistantWithPayload?.assistant_payload || null;
          if (isPipelineReady(payload)) {
            setPipeline({
              status: payload.status,
              message_ru: payload.message_ru,
              chat_reply_ru: payload.chat_reply_ru || payload.message_ru,
              pipeline_id: payload.pipeline_id,
              nodes: payload.nodes,
              edges: payload.edges,
              missing_requirements: payload.missing_requirements || [],
              context_summary: payload.context_summary,
            });
          } else {
            setPipeline(null);
          }
        } else {
          setMessages([{ role: 'assistant', content: DEFAULT_ASSISTANT_MESSAGE }]);
          // History is empty, but if we have initialMessage, handleSend will call setPipeline soon.
          if (!initialMessage) {
            setPipeline(null);
          }
        }
      } catch (error) {
        if (!cancelled) {
          // Preserve selected dialog ID and show an empty state instead of forcing a new dialog.
          setMessages([{ role: 'assistant', content: DEFAULT_ASSISTANT_MESSAGE }]);
          if (!initialMessage) {
            setPipeline(null);
          }
        }
      } finally {
        if (!cancelled) {
          setIsHydrating(false);
          setContextHydrating(false);
        }
      }
    };

    hydrateDialog();

    return () => {
      cancelled = true;
    };
  }, [initialDialogId, initialMessage, setPipeline, storageKey]);

  const handleSend = useCallback(
    async (overrideValue?: string) => {
      const valueToSend = overrideValue || inputValue;
      if (!valueToSend.trim() || isTyping || isHydrating) {
        return;
      }

      let activeDialogId = dialogId;
      if (!activeDialogId) {
        activeDialogId = generateUUID();
        setDialogId(activeDialogId);
        localStorage.setItem(storageKey, activeDialogId);
      }

      const userMessage = valueToSend;
      setMessages((prev) => [
        ...prev,
        { role: 'user', content: userMessage },
        {
          role: 'assistant',
          content: ASSISTANT_THINKING_MESSAGE,
          isGenerating: true,
        },
      ]);
      setInputValue('');
      setIsTyping(true);

      try {
        const response = await generatePipeline({
          dialog_id: activeDialogId,
          message: userMessage,
          capability_ids: null,
        });

        setMessages((prev) => {
          const newMessages = [...prev];
          const lastIndex = newMessages.length - 1;
          newMessages[lastIndex] = {
            role: 'assistant',
            content:
              response.chat_reply_ru ||
              response.message_ru ||
              (response.status === 'ready'
                ? 'Я подготовил Pipeline для вашей задачи.'
                : 'Произошла ошибка при генерации.'),
            isGenerating: false,
          };
          return newMessages;
        });

        // Invalidate history list to show new dialog
        queryClient.invalidateQueries({ queryKey: ["pipelineDialogs"] });

        if (isPipelineReady(response)) {
          setPipeline({
            status: response.status,
            message_ru: response.message_ru,
            chat_reply_ru: response.chat_reply_ru || response.message_ru,
            pipeline_id: response.pipeline_id,
            nodes: response.nodes,
            edges: response.edges,
            missing_requirements: response.missing_requirements || [],
            context_summary: response.context_summary,
          });
        } else {
          setPipeline(null);
        }

        if ((response.status === 'ready' || response.status === 'success') && onSynthesize) {
          onSynthesize(userMessage);
        }
      } catch (error) {
        console.error('Error in chat:', error);
        setPipeline(null);
        setMessages((prev) => {
          const newMessages = [...prev];
          const lastIndex = newMessages.length - 1;
          newMessages[lastIndex] = {
            role: 'assistant',
            content:
              'К сожалению, произошла ошибка при сборке пайплайна. Попробуйте перефразировать запрос.',
            isGenerating: false,
          };
          return newMessages;
        });
      } finally {
        setIsTyping(false);
      }
    },
    [dialogId, inputValue, isHydrating, isTyping, onSynthesize, setPipeline, storageKey]
  );

  useEffect(() => {
    if (!initialMessage || isHydrating || initialMessageProcessed.current) {
      return;
    }
    // Only send the initial message if we have no real messages yet (history is empty)
    const hasRealMessages = 
      messages.length > 1 || 
      (messages.length === 1 && messages[0].content !== DEFAULT_ASSISTANT_MESSAGE);
      
    if (!hasRealMessages) {
      initialMessageProcessed.current = true;
      handleSend(initialMessage);
    }
  }, [handleSend, initialMessage, isHydrating, messages]);

  const handleStartNewChat = () => {
    const newDialogId = generateUUID();
    setDialogId(newDialogId);
    localStorage.setItem(storageKey, newDialogId);
    setMessages([{ role: 'assistant', content: DEFAULT_ASSISTANT_MESSAGE }]);
    setPipeline(null);
    setInputValue('');
  };

  return (
    <div className={cn('flex flex-col h-full bg-card border-l border-border', className)}>
      <div className="p-4 border-b border-border flex items-center justify-between bg-muted/30">
        <div className="flex items-center gap-2">
          <Sparkles className="h-4 w-4 text-primary" />
          <h3 className="font-semibold text-sm text-foreground">Synthesis Chat</h3>
        </div>
        <div className="flex items-center gap-2">
          <Badge variant="outline" className="text-[10px] bg-primary/10 text-primary border-primary/20">
            AI ASSISTANT
          </Badge>
          {onClose && (
            <Button
              variant="ghost"
              size="icon"
              className="h-8 w-8 text-muted-foreground hover:text-foreground"
              onClick={onClose}
            >
              <X className="h-4 w-4" />
            </Button>
          )}
        </div>
      </div>

      <ScrollArea className="flex-1 p-4" ref={scrollRef}>
        <div className="space-y-4">
          {messages.map((msg, i) => (
            <div
              key={i}
              className={cn('flex gap-3 max-w-[90%]', msg.role === 'user' ? 'ml-auto flex-row-reverse' : '')}
            >
              <div
                className={cn(
                  'w-8 h-8 rounded-full flex items-center justify-center shrink-0 border border-border',
                  msg.role === 'assistant' ? 'bg-primary/10' : 'bg-muted'
                )}
              >
                {msg.role === 'assistant' ? (
                  <Bot className="h-4 w-4 text-primary" />
                ) : (
                  <User className="h-4 w-4" />
                )}
              </div>
              <div
                className={cn(
                  'p-3 rounded-2xl text-sm leading-relaxed shadow-sm border',
                  msg.role === 'assistant'
                    ? 'bg-card border-border text-foreground'
                    : 'bg-primary text-primary-foreground border-primary'
                )}
              >
                {msg.content}
                {msg.isGenerating && (
                  <span className="inline-flex ml-2 gap-1">
                    <span className="w-1 h-1 bg-primary rounded-full animate-bounce" />
                    <span className="w-1 h-1 bg-primary rounded-full animate-bounce [animation-delay:0.2s]" />
                    <span className="w-1 h-1 bg-primary rounded-full animate-bounce [animation-delay:0.4s]" />
                  </span>
                )}
              </div>
            </div>
          ))}
        </div>
      </ScrollArea>

      <div className="p-4 border-t border-border space-y-3">
        {messages.length > 1 && (
          <div className="flex gap-2 mb-2">
            <Button
              variant="outline"
              size="sm"
              className="h-7 text-[10px] gap-1 border-border"
              onClick={handleStartNewChat}
            >
              <RotateCcw className="h-3 w-3" /> Новый чат
            </Button>
          </div>
        )}
        <div className="relative">
          <Input
            placeholder="Опишите задачу..."
            className="pr-12 bg-background border-border h-11"
            value={inputValue}
            onChange={(e) => setInputValue(e.target.value)}
            onKeyDown={(e) => e.key === 'Enter' && handleSend()}
            disabled={isTyping || isHydrating}
          />
          <Button
            size="sm"
            className="absolute right-1 top-1 h-9 w-9 p-0 bg-primary hover:bg-primary/90"
            onClick={() => handleSend()}
            disabled={isTyping || isHydrating}
          >
            <Send className="h-4 w-4" />
          </Button>
        </div>
      </div>
    </div>
  );
};
