import React, { useEffect, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import {
  MessageSquare,
  Clock,
  Workflow,
  ChevronRight,
  Search,
  X
} from 'lucide-react';
import { format } from 'date-fns';
import { ru } from 'date-fns/locale';
import { listPipelineDialogs, PipelineDialogListItem } from '@/api/chat';
import { Input } from '@/components/ui/input';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { ScrollArea } from '@/components/ui/scroll-area';
import { Skeleton } from '@/components/ui/skeleton';
import { cn, generateUUID } from '@/lib/utils';
import { motion, AnimatePresence } from 'framer-motion';

import { useQuery } from '@tanstack/react-query';

interface HistoryDrawerProps {
  isOpen: boolean;
  onClose: () => void;
}

export const HistoryDrawer: React.FC<HistoryDrawerProps> = ({ isOpen, onClose }) => {
  const [searchQuery, setSearchQuery] = useState('');
  const navigate = useNavigate();

  const {
    data: dialogs = [],
    isLoading,
    refetch
  } = useQuery({
    queryKey: ['pipelineDialogs'],
    queryFn: () => listPipelineDialogs(50, 0),
    enabled: isOpen,
    refetchInterval: 5000, // Refresh every 5 seconds for real-time feel
  });

  const filteredDialogs = dialogs.filter(dialog =>
    dialog.title?.toLowerCase().includes(searchQuery.toLowerCase()) ||
    dialog.last_message_preview?.toLowerCase().includes(searchQuery.toLowerCase())
  );

  const handleOpenDialog = (dialogId: string) => {
    // Save to localStorage so SynthesisChat knows which one to load
    const storageKey = `pipeline_active_dialog_id:${JSON.parse(localStorage.getItem('user_data') || '{}')?.id || 'anonymous'
      }`;
    localStorage.setItem(storageKey, dialogId);
    navigate('/pipelines', { state: { dialogId } });
    onClose();
  };

  const getStatusBadge = (status: string | null) => {
    if (!status) return null;

    const variants: Record<string, string> = {
      'ready': 'bg-emerald-500/10 text-emerald-600 border-emerald-500/20',
      'success': 'bg-blue-500/10 text-blue-600 border-blue-500/20',
      'needs_input': 'bg-amber-500/10 text-amber-600 border-amber-500/20',
      'cannot_build': 'bg-rose-500/10 text-rose-600 border-rose-500/20',
      'error': 'bg-rose-500/10 text-rose-600 border-rose-500/20',
    };

    return (
      <Badge variant="outline" className={cn("text-[9px] h-4 px-1 px-1.5 font-medium uppercase tracking-wider", variants[status] || 'bg-muted text-muted-foreground')}>
        {status}
      </Badge>
    );
  };

  return (
    <AnimatePresence initial={false}>
      {isOpen && (
        <motion.div
          initial={{ width: 0, opacity: 0 }}
          animate={{ width: 320, opacity: 1 }}
          exit={{ width: 0, opacity: 0 }}
          transition={{ duration: 0.3, ease: [0.4, 0, 0.2, 1] }}
          className="h-full flex flex-col bg-card border-r border-border relative z-20 overflow-hidden"
        >
          <div className="w-[320px] h-full flex flex-col">
            <div className="p-4 border-b border-border bg-muted/20">
              <div className="flex items-center justify-between mb-4">
                <div className="flex items-center gap-2">
                  <div className="p-1.5 rounded-lg bg-primary/10">
                    <Clock className="h-4 w-4 text-primary" />
                  </div>
                  <h3 className="font-bold text-lg">История</h3>
                </div>
                <Button variant="ghost" size="icon" className="h-8 w-8 text-muted-foreground" onClick={onClose}>
                  <X className="h-4 w-4" />
                </Button>
              </div>

              <div className="relative">
                <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
                <Input
                  placeholder="Поиск..."
                  className="pl-9 h-9 bg-background border-border text-sm"
                  value={searchQuery}
                  onChange={(e) => setSearchQuery(e.target.value)}
                />
              </div>
            </div>

            <ScrollArea className="flex-1">
              <div className="p-2 space-y-1">
                {isLoading ? (
                  Array.from({ length: 6 }).map((_, i) => (
                    <div key={i} className="p-3 space-y-2">
                      <div className="flex gap-3">
                        <Skeleton className="h-8 w-8 rounded-lg" />
                        <div className="flex-1 space-y-1.5">
                          <Skeleton className="h-3 w-3/4" />
                          <Skeleton className="h-2 w-1/2" />
                        </div>
                      </div>
                    </div>
                  ))
                ) : filteredDialogs.length > 0 ? (
                  filteredDialogs.map((dialog) => (
                    <button
                      key={dialog.dialog_id}
                      onClick={() => handleOpenDialog(dialog.dialog_id)}
                      className="w-full text-left p-2.5 rounded-xl transition-all hover:bg-muted/50 group relative overflow-hidden"
                    >
                      <div className="flex gap-3 relative z-10">
                        <div className="w-8 h-8 rounded-lg bg-primary/5 border border-primary/10 flex items-center justify-center shrink-0 group-hover:bg-primary/10 transition-colors">
                          <MessageSquare className="h-4 w-4 text-primary/70 group-hover:text-primary transition-colors" />
                        </div>

                        <div className="flex-1 min-w-0">
                          <div className="flex items-center justify-between gap-2 mb-0.5">
                            <span className="text-xs font-semibold text-foreground truncate block group-hover:text-primary transition-colors">
                              {dialog.title || 'Новый диалог'}
                            </span>
                            <span className="text-[9px] text-muted-foreground whitespace-nowrap">
                              {format(new Date(dialog.updated_at), 'HH:mm', { locale: ru })}
                            </span>
                          </div>

                          <p className="text-[11px] text-muted-foreground line-clamp-1 group-hover:text-muted-foreground/80 transition-colors">
                            {dialog.last_message_preview || 'Нет сообщений'}
                          </p>

                          <div className="flex items-center gap-2 mt-1">
                            {getStatusBadge(dialog.last_status)}
                            {dialog.last_pipeline_id && (
                              <span className="flex items-center gap-0.5 text-[8px] text-primary/70 font-medium font-bold uppercase tracking-wider">
                                <Workflow className="h-2.5 w-2.5" />
                                Flow
                              </span>
                            )}
                            <span className="text-blue-500/70 opacity-0 group-hover:opacity-100 transition-opacity text-[9px] font-medium ml-auto flex items-center gap-0.5">
                              Открыть <ChevronRight className="h-2.5 w-2.5" />
                            </span>
                          </div>
                        </div>
                      </div>
                    </button>
                  ))
                ) : (
                  <div className="p-8 text-center space-y-2">
                    <div className="w-10 h-10 bg-muted rounded-full flex items-center justify-center mx-auto mb-2">
                      <MessageSquare className="h-5 w-5 text-muted-foreground" />
                    </div>
                    <p className="text-sm font-medium text-foreground">Диалоги не найдены</p>
                  </div>
                )}
              </div>
            </ScrollArea>

            <div className="p-3 border-t border-border bg-muted/10">
              <Button
                className="w-full h-9 gap-2 text-xs"
                variant="outline"
                onClick={() => {
                  const newId = generateUUID();
                  const storageKey = `pipeline_active_dialog_id:${JSON.parse(localStorage.getItem('user_data') || '{}')?.id || 'anonymous'
                    }`;
                  localStorage.setItem(storageKey, newId);
                  navigate('/pipelines', { state: { dialogId: newId } });
                  onClose();
                }}
              >
                <MessageSquare className="h-3.5 w-3.5" /> Новый диалог
              </Button>
            </div>
          </div>
        </motion.div>
      )}
    </AnimatePresence>
  );
};
