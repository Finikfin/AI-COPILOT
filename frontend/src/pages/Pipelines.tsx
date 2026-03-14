import React from 'react';
import { useLocation } from 'react-router-dom';
import { SynthesisChat } from '@/components/shared/SynthesisChat';
import { Card } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Play, Settings, Database, ArrowRight, Activity, Zap } from 'lucide-react';

export const Pipelines: React.FC = () => {
  const location = useLocation();
  const initialMessage = location.state?.initialMessage;

  return (
    <div className="h-full flex overflow-hidden">
      {/* Main Pipeline Zone - Center */}
      <div className="flex-1 relative bg-muted/5 bg-grid-pattern p-8 overflow-auto">
        <div className="max-w-4xl mx-auto space-y-12 py-10">
          <div className="flex flex-col items-center mb-12">
            <h1 className="text-2xl font-bold text-foreground mb-2">Editor Pipeline</h1>
            <p className="text-sm text-muted-foreground">Визуализация текущего процесса автоматизации</p>
          </div>

          <div className="relative flex items-center justify-between gap-4 px-10">
            {/* Nodes are connected by ArrowRight icons */}

            {/* Node 1: Start */}
            <Card className="relative z-10 w-52 p-4 border-primary/20 bg-card shadow-lg flex flex-col items-center gap-3">
              <div className="h-10 w-10 rounded-full bg-primary/10 flex items-center justify-center text-primary">
                <Zap className="h-5 w-5" />
              </div>
              <div className="text-center">
                <p className="text-sm font-semibold">Start Trigger</p>
                <p className="text-[10px] text-muted-foreground">HTTP Webhook</p>
              </div>
              <Badge variant="outline" className="text-[10px] bg-green-500/10 text-green-600 border-green-500/20">
                Active
              </Badge>
            </Card>

            {/* Connection Line 1-2 */}
            <div className="flex-1 h-0.5 bg-border/50 relative min-w-[30px]">
              <div className="absolute right-0 top-1/2 -translate-y-1/2 flex items-center" />
            </div>

            {/* Node 2: Process */}
            <Card className="relative z-10 w-52 p-4 border-primary/20 bg-card shadow-lg flex flex-col items-center gap-3">
              <div className="h-10 w-10 rounded-full bg-blue-500/10 flex items-center justify-center text-blue-500">
                <Settings className="h-5 w-5" />
              </div>
              <div className="text-center">
                <p className="text-sm font-semibold">Data Mapper</p>
                <p className="text-[10px] text-muted-foreground">Transformation</p>
              </div>
              <Badge variant="outline" className="text-[10px] bg-blue-500/10 text-blue-600 border-blue-500/20">
                Processing
              </Badge>
            </Card>

            {/* Connection Line 2-3 */}
            <div className="flex-1 h-0.5 bg-border/50 relative min-w-[30px]">
              <div className="absolute right-0 top-1/2 -translate-y-1/2 flex items-center" />
            </div>

            {/* Node 3: Database */}
            <Card className="relative z-10 w-52 p-4 border-primary/20 bg-card shadow-lg flex flex-col items-center gap-3">
              <div className="h-10 w-10 rounded-full bg-purple-500/10 flex items-center justify-center text-purple-500">
                <Database className="h-5 w-5" />
              </div>
              <div className="text-center">
                <p className="text-sm font-semibold">Store Record</p>
                <p className="text-[10px] text-muted-foreground">PostgreSQL</p>
              </div>
              <Badge variant="outline" className="text-[10px] bg-muted text-muted-foreground border-border">
                Idle
              </Badge>
            </Card>
          </div>

          <Card className="mt-20 p-6 bg-primary/5 border-dashed border-primary/20 flex items-center justify-between">
            <div className="flex items-center gap-4">
              <div className="h-12 w-12 rounded-xl bg-primary/10 flex items-center justify-center text-primary">
                <Activity className="h-6 w-6" />
              </div>
              <div>
                <p className="font-semibold text-foreground">Статус пайплайна</p>
                <p className="text-sm text-muted-foreground">Все модули в режиме ожидания запуска</p>
              </div>
            </div>
            <Button className="gap-2">
              <Play className="h-4 w-4" /> Запустить поток
            </Button>
          </Card>
        </div>
      </div>

      {/* Right Sidebar - AI Chat */}
      <SynthesisChat
        className="w-80"
        initialMessage={initialMessage}
      />
    </div>
  );
};

export default Pipelines;
