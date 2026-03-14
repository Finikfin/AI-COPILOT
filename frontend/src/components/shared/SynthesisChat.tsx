import React, { useState, useRef, useEffect } from 'react';
import { 
  Send, 
  Sparkles, 
  User, 
  Bot, 
  RotateCcw, 
  CheckCircle2
} from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { ScrollArea } from '@/components/ui/scroll-area';
import { Badge } from '@/components/ui/badge';
import { cn, generateUUID } from '@/lib/utils';
import { generatePipeline } from '@/api/chat';

interface Message {
  role: 'user' | 'assistant';
  content: string;
  isGenerating?: boolean;
}

interface SynthesisChatProps {
  onSynthesize?: (prompt: string) => void;
  className?: string;
  initialMessage?: string;
  initialDialogId?: string;
}

export const SynthesisChat: React.FC<SynthesisChatProps> = ({ 
  onSynthesize, 
  className, 
  initialMessage,
  initialDialogId 
}) => {
  const [messages, setMessages] = useState<Message[]>([
    { 
      role: 'assistant', 
      content: 'Привет! Я помогу собрать Pipeline. Опишите бизнес-задачу, которую хотите автоматизировать.' 
    }
  ]);
  const [inputValue, setInputValue] = useState('');
  const [dialogId] = useState<string>(initialDialogId || generateUUID());
  const scrollRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [messages]);

  // Handle initial message from props
  useEffect(() => {
    if (initialMessage && messages.length === 1) {
      handleSend(initialMessage);
    }
  }, [initialMessage]);

  const handleSend = async (overrideValue?: string) => {
    const valueToSend = overrideValue || inputValue;
    if (!valueToSend.trim()) return;

    const userMessage = valueToSend;
    setMessages(prev => [...prev, { role: 'user', content: userMessage }]);
    setInputValue('');

    // Pre-add assistant message with loading state
    setMessages(prev => [...prev, { 
      role: 'assistant', 
      content: 'Анализирую возможности...',
      isGenerating: true 
    }]);

    try {
      // Send message to generate pipeline endpoint
      const response = await generatePipeline({
        dialog_id: dialogId,
        message: userMessage,
        user_id: null,
        capability_ids: null
      });

      setMessages(prev => {
        const newMessages = [...prev];
        const lastIndex = newMessages.length - 1;
        newMessages[lastIndex] = { 
          role: 'assistant', 
          content: response.message_ru || (response.status === 'success' ? 'Я подготовил Pipeline для вашей задачи.' : 'Произошла ошибка при генерации.'),
          isGenerating: false 
        };
        return newMessages;
      });

      if (response.status === 'success' && onSynthesize) {
        onSynthesize(userMessage);
      }
    } catch (error) {
      console.error('Error in chat:', error);
      setMessages(prev => {
        const newMessages = [...prev];
        const lastIndex = newMessages.length - 1;
        newMessages[lastIndex] = { 
          role: 'assistant', 
          content: 'К сожалению, произошла ошибка сетевого соединения. Попробуйте еще раз.',
          isGenerating: false 
        };
        return newMessages;
      });
    }
  };

  return (
    <div className={cn("flex flex-col h-full bg-card border-l border-border", className)}>
      <div className="p-4 border-b border-border flex items-center justify-between bg-muted/30">
        <div className="flex items-center gap-2">
          <Sparkles className="h-4 w-4 text-primary" />
          <h3 className="font-semibold text-sm text-foreground">Synthesis Chat</h3>
        </div>
        <Badge variant="outline" className="text-[10px] bg-primary/10 text-primary border-primary/20">
          AI ASSISTANT
        </Badge>
      </div>

      <ScrollArea className="flex-1 p-4" ref={scrollRef}>
        <div className="space-y-4">
          {messages.map((msg, i) => (
            <div key={i} className={cn(
              "flex gap-3 max-w-[90%]",
              msg.role === 'user' ? "ml-auto flex-row-reverse" : ""
            )}>
              <div className={cn(
                "w-8 h-8 rounded-full flex items-center justify-center shrink-0 border border-border",
                msg.role === 'assistant' ? "bg-primary/10" : "bg-muted"
              )}>
                {msg.role === 'assistant' ? <Bot className="h-4 w-4 text-primary" /> : <User className="h-4 w-4" />}
              </div>
              <div className={cn(
                "p-3 rounded-2xl text-sm leading-relaxed shadow-sm border",
                msg.role === 'assistant' 
                  ? "bg-card border-border text-foreground" 
                  : "bg-primary text-primary-foreground border-primary"
              )}>
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
        {messages.length > 2 && (
          <div className="flex gap-2 mb-2">
             <Button variant="outline" size="sm" className="h-7 text-[10px] gap-1 border-border">
               <RotateCcw className="h-3 w-3" /> Пересобрать
             </Button>
             <Button variant="outline" size="sm" className="h-7 text-[10px] gap-1 border-border text-primary">
               <CheckCircle2 className="h-3 w-3" /> Подтвердить
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
          />
          <Button 
            size="sm" 
            className="absolute right-1 top-1 h-9 w-9 p-0 bg-primary hover:bg-primary/90"
            onClick={() => handleSend()}
          >
            <Send className="h-4 w-4" />
          </Button>
        </div>
      </div>
    </div>
  );
};
