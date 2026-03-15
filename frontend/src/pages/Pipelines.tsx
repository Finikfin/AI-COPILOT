import React from 'react';
import { useLocation } from 'react-router-dom';
import { SynthesisChat } from '@/components/shared/SynthesisChat';
import { Card } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Play, Settings, Database, Activity, Zap, Box, Server, ArrowRight } from 'lucide-react';
import { usePipelineContext } from '@/contexts/PipelineContext';
import { cn } from '@/lib/utils';

export const Pipelines: React.FC = () => {
  const location = useLocation();
  const initialMessage = location.state?.initialMessage;
  const dialogId = location.state?.dialogId;
  const { currentPipeline } = usePipelineContext();

  const getNodeIcon = (index: number) => {
    switch (index % 5) {
      case 0: return <Zap className="h-5 w-5" />;
      case 1: return <Settings className="h-5 w-5" />;
      case 2: return <Database className="h-5 w-5" />;
      case 3: return <Server className="h-5 w-5" />;
      case 4: return <Activity className="h-5 w-5" />;
      default: return <Box className="h-5 w-5" />;
    }
  };

  const getNodeColor = (index: number) => {
    const colors = [
      'text-primary bg-primary/10',
      'text-blue-500 bg-blue-500/10',
      'text-purple-500 bg-purple-500/10',
      'text-orange-500 bg-orange-500/10',
      'text-green-500 bg-green-500/10',
    ];
    return colors[index % colors.length];
  };

  return (
    <div className="h-full flex overflow-hidden">
      {/* Main Pipeline Zone - Center */}
      <div className="flex-1 relative bg-muted/5 bg-grid-pattern p-8 overflow-auto">
        <div className="max-w-6xl mx-auto space-y-12 py-10">
          <div className="flex flex-col items-center mb-12 text-center">
            <h1 className="text-3xl font-bold text-foreground mb-2">Editor Pipeline</h1>
            {currentPipeline ? (
               <div className="flex flex-col items-center">
                 <p className="text-sm text-primary font-medium">Pipeline ID: {currentPipeline.pipeline_id}</p>
                 <p className="text-xs text-muted-foreground mt-1 max-w-lg italic">"{currentPipeline.context_summary}"</p>
               </div>
            ) : (
              <p className="text-sm text-muted-foreground">Визуализация текущего процесса автоматизации</p>
            )}
          </div>

          <div className="flex flex-wrap items-center justify-center gap-y-12 gap-x-4 px-4 min-h-[300px]">
            {currentPipeline && currentPipeline.nodes.length > 0 ? (
              currentPipeline.nodes.map((node, index) => (
                <React.Fragment key={node.step}>
                  <Card className="relative z-10 w-64 p-5 border-border hover:border-primary/40 transition-all bg-card/80 backdrop-blur-sm shadow-xl flex flex-col items-center gap-3 animate-in fade-in zoom-in duration-500">
                    <div className={cn("h-12 w-12 rounded-2xl flex items-center justify-center shadow-inner", getNodeColor(index))}>
                      {getNodeIcon(index)}
                    </div>
                    <div className="text-center">
                      <p className="text-sm font-bold text-foreground line-clamp-1">{node.name}</p>
                      <p className="text-[11px] text-muted-foreground line-clamp-3 mt-1.5 leading-relaxed">
                        {node.description}
                      </p>
                    </div>
                    
                    <div className="w-full h-px bg-border/50 my-1" />
                    
                    <div className="flex flex-wrap gap-1.5 justify-center">
                      {node.endpoints.map((ep, epIdx) => (
                        <Badge 
                          key={epIdx} 
                          variant="secondary" 
                          className="text-[9px] px-1.5 py-0 font-medium bg-muted/50 text-muted-foreground border-transparent"
                        >
                          {ep.name}
                        </Badge>
                      ))}
                    </div>
                    
                    {node.external_inputs.length > 0 && (
                       <div className="flex items-center gap-1 mt-1">
                         <span className="text-[9px] text-muted-foreground font-semibold uppercase tracking-wider">Inputs:</span>
                         {node.external_inputs.map((input, idx) => (
                           <Badge key={idx} className="text-[9px] px-1.5 py-0 bg-primary/5 text-primary border-primary/20">
                             {input}
                           </Badge>
                         ))}
                       </div>
                    )}
                  </Card>

                  {/* Horizontal Connector Arrow */}
                  {index < currentPipeline.nodes.length - 1 && (
                    <div className="hidden md:flex items-center justify-center text-muted-foreground/30 animate-in fade-in slide-in-from-left-4 duration-700">
                      <ArrowRight className="h-6 w-6 stroke-[3px]" />
                    </div>
                  )}
                </React.Fragment>
              ))
            ) : (
              <div className="flex flex-col items-center justify-center py-20 text-center opacity-40">
                <Box className="h-20 w-20 mb-6 text-muted-foreground stroke-[1px]" />
                <h2 className="text-xl font-semibold text-foreground">Пайплайн пуст</h2>
                <p className="text-sm mt-2 max-w-xs">
                  Опишите бизнес-задачу в Synthesis Chat справа, чтобы AI собрал последовательность шагов.
                </p>
              </div>
            )}
          </div>

          {currentPipeline && (
            <div className="max-w-xl mx-auto">
              <Card className="mt-8 p-6 bg-primary/5 border-dashed border-primary/20 flex items-center justify-between shadow-lg animate-in fade-in slide-in-from-bottom-8 duration-700">
                <div className="flex items-center gap-5">
                  <div className="h-14 w-14 rounded-2xl bg-primary/10 flex items-center justify-center text-primary shadow-sm">
                    <Play className="h-7 w-7 fill-primary/20" />
                  </div>
                  <div>
                    <p className="font-bold text-lg text-foreground">Пайплайн готов к работе</p>
                    <p className="text-sm text-muted-foreground">Все технические зависимости соблюдены</p>
                  </div>
                </div>
                <Button size="lg" className="gap-2 px-8 shadow-primary/20 hover:shadow-primary/40 transition-shadow">
                  <Play className="h-5 w-5" /> Запустить
                </Button>
              </Card>
            </div>
          )}
        </div>
      </div>

      {/* Right Sidebar - AI Chat */}
      <SynthesisChat
        className="w-80"
        initialMessage={initialMessage}
        initialDialogId={dialogId}
      />
    </div>
  );
};

export default Pipelines;
