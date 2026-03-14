import React, { useState } from 'react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { FileJson, Send, Sparkles, Wand2, Shield, Zap } from 'lucide-react';
import { SwaggerImportModal } from '@/components/shared/SwaggerImportModal';
import { ImportResultsModal } from '@/components/shared/ImportResultsModal';
import { useNavigate } from 'react-router-dom';
import { Action } from '@/types/action';
import { useActionsContext } from '@/contexts/ActionContext';
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip";

const Home: React.FC = () => {
  const { actions, addActions } = useActionsContext();
  const [isImportModalOpen, setIsImportModalOpen] = useState(false);
  const [isResultsModalOpen, setIsResultsModalOpen] = useState(false);
  const [importResults, setImportResults] = useState<{ success_actions: Action[], failed_actions: any[] } | null>(null);
  const [chatMessage, setChatMessage] = useState('');
  const navigate = useNavigate();

  const isChatDisabled = actions.length === 0;

  const handleSendMessage = (e: React.FormEvent) => {
    e.preventDefault();
    if (!chatMessage.trim()) return;
    // Logic for chat can be added here
    setChatMessage('');
  };

  return (
    <div className="min-h-full flex flex-col items-center justify-center px-4 py-12 bg-grid-pattern relative overflow-hidden">
      {/* Background Glows */}
      <div className="absolute top-1/4 left-1/4 w-64 h-64 bg-primary/20 rounded-full blur-[120px] -z-10 animate-pulse" />
      <div className="absolute bottom-1/4 right-1/4 w-64 h-64 bg-blue-500/10 rounded-full blur-[120px] -z-10" />

      <div className="max-w-4xl w-full text-center space-y-8 relative">
        {/* Header Section */}
        <div className="space-y-4 animate-in fade-in slide-in-from-bottom-4 duration-700">
          <div className="inline-flex items-center gap-2 px-3 py-1 rounded-full bg-primary/10 border border-primary/20 text-primary text-xs font-semibold mb-2">
            <Sparkles className="h-3.5 w-3.5" />
            <span>Next Generation AI Copilot</span>
          </div>
          <h1 className="text-5xl md:text-7xl font-bold tracking-tight text-foreground">
            Ai <span className="text-primary">Copilot</span>
          </h1>
          <p className="text-xl text-muted-foreground max-w-2xl mx-auto">
            Интеллектуальный помощник для управления вашими API и автоматизации рабочих процессов с помощью ИИ.
          </p>
        </div>

        {/* Chat Input Section */}
        <div className="max-w-2xl mx-auto w-full pt-8 animate-in fade-in slide-in-from-bottom-6 duration-700 delay-150">
          <form
            onSubmit={handleSendMessage}
            className="relative group border border-border bg-card/50 backdrop-blur-xl rounded-2xl shadow-2xl p-2 focus-within:ring-2 focus-within:ring-primary/30 transition-all duration-300"
          >
            <Input
              value={chatMessage}
              onChange={(e) => setChatMessage(e.target.value)}
              placeholder="Как я могу помочь вам с вашими API сегодня?"
              className="bg-transparent border-none shadow-none h-10 pl-4 pr-16 text-lg focus-visible:ring-0"
            />
            <Tooltip>
              <TooltipTrigger asChild>
                <div className="absolute right-2 top-2">
                  <Button
                    type="submit"
                    size="icon"
                    disabled={isChatDisabled}
                    className={`h-10 w-10 rounded-xl transition-transform active:scale-95 ${
                      isChatDisabled 
                      ? 'bg-muted text-muted-foreground cursor-not-allowed opacity-50' 
                      : 'bg-primary hover:bg-primary/90'
                    }`}
                  >
                    <Send className="h-5 w-5" />
                  </Button>
                </div>
              </TooltipTrigger>
              {isChatDisabled && (
                <TooltipContent side="top" className="bg-popover text-popover-foreground border-border shadow-xl">
                  <p>Сначала импортируйте Swagger спецификацию</p>
                </TooltipContent>
              )}
            </Tooltip>
          </form>
          <div className="flex flex-wrap items-center justify-center gap-4 mt-6">
            <Button
              variant="outline"
              className="gap-2 border-border bg-card/50 backdrop-blur-sm hover:bg-accent transition-all animate-in fade-in zoom-in duration-500 delay-300"
              onClick={() => setIsImportModalOpen(true)}
            >
              <FileJson className="h-4 w-4 text-primary" />
              Import Swagger
            </Button>
          </div>
        </div>

        {/* Features Preview */}
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6 pt-16 animate-in fade-in slide-in-from-bottom-8 duration-700 delay-300">
          <div className="p-6 rounded-2xl border border-border bg-card/30 backdrop-blur-sm hover:border-primary/50 transition-colors group">
            <div className="w-10 h-10 rounded-lg bg-primary/10 flex items-center m-auto justify-center mb-4 group-hover:scale-110 transition-transform">
              <Zap className="h-5 w-5 text-primary" />
            </div>
            <h3 className="text-lg font-semibold mb-2">Быстрый импорт</h3>
            <p className="text-sm text-muted-foreground">Загружайте OpenAPI спецификации и начинайте работу за секунды.</p>
          </div>
          <div className="p-6 rounded-2xl border border-border bg-card/30 backdrop-blur-sm hover:border-primary/50 transition-colors group">
            <div className="w-10 h-10 rounded-lg bg-blue-500/10 flex m-auto items-center justify-center mb-4 group-hover:scale-110 transition-transform">
              <Wand2 className="h-5 w-5 text-blue-500" />
            </div>
            <h3 className="text-lg font-semibold mb-2">AI Генерация</h3>
            <p className="text-sm text-muted-foreground">Создавайте сложные пайплайны и логику с помощью простых текстовых запросов.</p>
          </div>
          <div className="p-6 rounded-2xl border border-border bg-card/30 backdrop-blur-sm hover:border-primary/50 transition-colors group">
            <div className="w-10 h-10 rounded-lg bg-purple-500/10 m-auto flex items-center justify-center mb-4 group-hover:scale-110 transition-transform">
              <Shield className="h-5 w-5 text-purple-500" />
            </div>
            <h3 className="text-lg font-semibold mb-2">Безопасность</h3>
            <p className="text-sm text-muted-foreground">Ваши данные обрабатываются локально и защищены современными стандартами.</p>
          </div>
        </div>
      </div>

      {/* Modals */}
      <SwaggerImportModal
        isOpen={isImportModalOpen}
        onClose={() => setIsImportModalOpen(false)}
        onImport={(data) => {
          if (data && (data.success_actions || data.actions)) {
            const successList = data.success_actions || data.actions || [];
            const failedList = data.failed_actions || [];
            
            // Update global context with successful actions
            addActions(successList);

            setImportResults({
              success_actions: successList,
              failed_actions: failedList
            });
            setIsResultsModalOpen(true);
          }
        }}
      />

      <ImportResultsModal
        isOpen={isResultsModalOpen}
        onClose={() => {
          setIsResultsModalOpen(false);
          // Redirect to actions page after closing results if there were successes
          if (importResults && importResults.success_actions.length > 0) {
            navigate('/actions');
          }
        }}
        results={importResults}
      />
    </div>
  );
};

export default Home;
