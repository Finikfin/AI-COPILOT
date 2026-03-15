import React from 'react';
import { useLocation } from 'react-router-dom';
import { SynthesisChat } from '@/components/shared/SynthesisChat';
import { Card } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Play, Settings, Database, Activity, Zap, Box, Server } from 'lucide-react';
import { usePipelineContext } from '@/contexts/PipelineContext';
import { cn } from '@/lib/utils';
import { PipelineData, PipelineEdge, PipelineNode } from '@/types/pipeline';

type PositionedNode = {
  node: PipelineNode;
  x: number;
  y: number;
};

const CARD_WIDTH = 256;
const CARD_HEIGHT = 220;
const COLUMN_GAP = 120;
const ROW_GAP = 72;
const PADDING_X = 48;
const PADDING_Y = 32;

const buildGraphLayout = (pipeline: PipelineData) => {
  const nodesByStep = new Map<number, PipelineNode>();
  for (const node of pipeline.nodes) {
    nodesByStep.set(node.step, node);
  }

  const incomingCount = new Map<number, number>();
  const outgoing = new Map<number, number[]>();

  for (const node of pipeline.nodes) {
    incomingCount.set(node.step, 0);
    outgoing.set(node.step, []);
  }

  for (const edge of pipeline.edges) {
    if (!nodesByStep.has(edge.from_step) || !nodesByStep.has(edge.to_step)) {
      continue;
    }
    incomingCount.set(edge.to_step, (incomingCount.get(edge.to_step) || 0) + 1);
    outgoing.get(edge.from_step)?.push(edge.to_step);
  }

  const queue: number[] = [];
  const levels = new Map<number, number>();
  for (const node of pipeline.nodes) {
    if ((incomingCount.get(node.step) || 0) === 0) {
      queue.push(node.step);
      levels.set(node.step, 0);
    }
  }

  if (queue.length === 0) {
    for (const node of pipeline.nodes) {
      queue.push(node.step);
      levels.set(node.step, 0);
    }
  }

  while (queue.length > 0) {
    const step = queue.shift()!;
    const currentLevel = levels.get(step) || 0;
    for (const nextStep of outgoing.get(step) || []) {
      const nextLevel = currentLevel + 1;
      if (!levels.has(nextStep) || nextLevel > (levels.get(nextStep) || 0)) {
        levels.set(nextStep, nextLevel);
      }
      incomingCount.set(nextStep, (incomingCount.get(nextStep) || 0) - 1);
      if ((incomingCount.get(nextStep) || 0) <= 0) {
        queue.push(nextStep);
      }
    }
  }

  for (const node of pipeline.nodes) {
    if (!levels.has(node.step)) {
      levels.set(node.step, 0);
    }
  }

  const columns = new Map<number, PipelineNode[]>();
  for (const node of pipeline.nodes) {
    const level = levels.get(node.step) || 0;
    const columnNodes = columns.get(level) || [];
    columnNodes.push(node);
    columns.set(level, columnNodes);
  }

  const sortedLevels = Array.from(columns.keys()).sort((a, b) => a - b);
  for (const level of sortedLevels) {
    columns.get(level)?.sort((a, b) => a.step - b.step);
  }

  const positionedNodes: PositionedNode[] = [];
  let maxRows = 0;
  for (const level of sortedLevels) {
    const columnNodes = columns.get(level) || [];
    maxRows = Math.max(maxRows, columnNodes.length);
    columnNodes.forEach((node, rowIndex) => {
      positionedNodes.push({
        node,
        x: PADDING_X + level * (CARD_WIDTH + COLUMN_GAP),
        y: PADDING_Y + rowIndex * (CARD_HEIGHT + ROW_GAP),
      });
    });
  }

  const positionedByStep = new Map<number, PositionedNode>();
  for (const positionedNode of positionedNodes) {
    positionedByStep.set(positionedNode.node.step, positionedNode);
  }

  const drawableEdges = pipeline.edges
    .map((edge) => {
      const fromNode = positionedByStep.get(edge.from_step);
      const toNode = positionedByStep.get(edge.to_step);
      if (!fromNode || !toNode) {
        return null;
      }
      return {
        edge,
        fromX: fromNode.x + CARD_WIDTH,
        fromY: fromNode.y + CARD_HEIGHT / 2,
        toX: toNode.x,
        toY: toNode.y + CARD_HEIGHT / 2,
      };
    })
    .filter(Boolean) as Array<{
      edge: PipelineEdge;
      fromX: number;
      fromY: number;
      toX: number;
      toY: number;
    }>;

  const width = PADDING_X * 2 + sortedLevels.length * CARD_WIDTH + Math.max(0, sortedLevels.length - 1) * COLUMN_GAP;
  const height = PADDING_Y * 2 + Math.max(maxRows, 1) * CARD_HEIGHT + Math.max(0, maxRows - 1) * ROW_GAP;

  return {
    positionedNodes,
    drawableEdges,
    width,
    height,
  };
};

export const Pipelines: React.FC = () => {
  const location = useLocation();
  const initialMessage = location.state?.initialMessage;
  const dialogId = location.state?.dialogId;
  const { currentPipeline } = usePipelineContext();
  const graphLayout = currentPipeline ? buildGraphLayout(currentPipeline) : null;

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

          <div className="min-h-[300px] overflow-auto rounded-[28px] border border-border/40 bg-card/20 p-4">
            {currentPipeline && graphLayout && currentPipeline.nodes.length > 0 ? (
              <div
                className="relative mx-auto"
                style={{
                  width: `${graphLayout.width}px`,
                  height: `${graphLayout.height}px`,
                  minWidth: `${graphLayout.width}px`,
                }}
              >
                <svg
                  className="absolute inset-0 h-full w-full overflow-visible"
                  viewBox={`0 0 ${graphLayout.width} ${graphLayout.height}`}
                  fill="none"
                >
                  <defs>
                    <marker
                      id="pipeline-arrow"
                      markerWidth="10"
                      markerHeight="10"
                      refX="8"
                      refY="5"
                      orient="auto"
                    >
                      <path d="M0 0L10 5L0 10Z" fill="currentColor" />
                    </marker>
                  </defs>
                  {graphLayout.drawableEdges.map(({ edge, fromX, fromY, toX, toY }) => {
                    const midX = fromX + (toX - fromX) / 2;
                    const path = `M ${fromX} ${fromY} C ${midX} ${fromY}, ${midX} ${toY}, ${toX} ${toY}`;
                    return (
                      <g key={`${edge.from_step}-${edge.to_step}-${edge.type}`}>
                        <path
                          d={path}
                          className="text-primary/35"
                          stroke="currentColor"
                          strokeWidth="3"
                          strokeLinecap="round"
                          markerEnd="url(#pipeline-arrow)"
                        />
                        <text
                          x={midX}
                          y={(fromY + toY) / 2 - 8}
                          textAnchor="middle"
                          className="fill-muted-foreground text-[10px] uppercase tracking-[0.18em]"
                        >
                          {edge.type}
                        </text>
                      </g>
                    );
                  })}
                </svg>

                {graphLayout.positionedNodes.map(({ node, x, y }, index) => (
                  <Card
                    key={node.step}
                    className="absolute z-10 w-64 border-border hover:border-primary/40 bg-card/85 p-5 shadow-xl backdrop-blur-sm transition-all animate-in fade-in zoom-in duration-500"
                    style={{ left: `${x}px`, top: `${y}px`, height: `${CARD_HEIGHT}px` }}
                  >
                    <div className="flex h-full flex-col items-center gap-3 text-center">
                      <div className={cn("h-12 w-12 rounded-2xl flex items-center justify-center shadow-inner", getNodeColor(index))}>
                        {getNodeIcon(index)}
                      </div>
                      <div>
                        <p className="line-clamp-1 text-sm font-bold text-foreground">{node.name}</p>
                        <p className="mt-1.5 line-clamp-3 text-[11px] leading-relaxed text-muted-foreground">
                          {node.description}
                        </p>
                      </div>
                      <div className="my-1 w-full h-px bg-border/50" />
                      <div className="flex flex-wrap justify-center gap-1.5">
                        {node.endpoints.map((ep, epIdx) => (
                          <Badge
                            key={epIdx}
                            variant="secondary"
                            className="border-transparent bg-muted/50 px-1.5 py-0 text-[9px] font-medium text-muted-foreground"
                          >
                            {ep.name}
                          </Badge>
                        ))}
                      </div>
                      {node.external_inputs.length > 0 && (
                        <div className="mt-auto flex flex-wrap items-center justify-center gap-1">
                          <span className="text-[9px] font-semibold uppercase tracking-wider text-muted-foreground">
                            Inputs:
                          </span>
                          {node.external_inputs.map((input, idx) => (
                            <Badge key={idx} className="border-primary/20 bg-primary/5 px-1.5 py-0 text-[9px] text-primary">
                              {input}
                            </Badge>
                          ))}
                        </div>
                      )}
                    </div>
                  </Card>
                ))}
              </div>
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
