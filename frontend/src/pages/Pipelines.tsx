import React from 'react';
import { useLocation } from 'react-router-dom';
import { SynthesisChat } from '@/components/shared/SynthesisChat';
import { Card } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Activity, Play, Sparkles } from 'lucide-react';
import { usePipelineContext } from '@/contexts/PipelineContext';
import { PipelineData, PipelineEdge, PipelineNode } from '@/types/pipeline';
import { cn } from '@/lib/utils';

type PositionedNode = {
  node: PipelineNode;
  x: number;
  y: number;
};

type DrawableEdge = {
  edge: PipelineEdge;
  path: string;
  labelX: number;
  labelY: number;
};

const CARD_WIDTH = 256;
const CARD_HEIGHT = 220;
const COLUMN_GAP = 120;
const ROW_GAP = 72;
const PADDING_X = 48;
const PADDING_Y = 32;

const buildGraphLayout = (pipeline: PipelineData) => {
  const nodeByStep = new Map<number, PipelineNode>();
  const incoming = new Map<number, number>();
  const outgoing = new Map<number, number[]>();

  pipeline.nodes.forEach((node) => {
    nodeByStep.set(node.step, node);
    incoming.set(node.step, 0);
    outgoing.set(node.step, []);
  });

  pipeline.edges.forEach((edge) => {
    if (!nodeByStep.has(edge.from_step) || !nodeByStep.has(edge.to_step)) {
      return;
    }
    outgoing.get(edge.from_step)?.push(edge.to_step);
    incoming.set(edge.to_step, (incoming.get(edge.to_step) || 0) + 1);
  });

  const roots = pipeline.nodes
    .filter((node) => (incoming.get(node.step) || 0) === 0)
    .sort((left, right) => left.step - right.step);
  const levels = new Map<number, number>();
  const queue = [...roots];

  roots.forEach((node) => levels.set(node.step, 0));

  while (queue.length > 0) {
    const current = queue.shift();
    if (!current) {
      continue;
    }
    const currentLevel = levels.get(current.step) || 0;
    const children = outgoing.get(current.step) || [];
    children.forEach((childStep) => {
      const nextLevel = currentLevel + 1;
      const previousLevel = levels.get(childStep);
      if (previousLevel === undefined || nextLevel > previousLevel) {
        levels.set(childStep, nextLevel);
      }
      const childNode = nodeByStep.get(childStep);
      if (childNode) {
        queue.push(childNode);
      }
    });
  }

  pipeline.nodes.forEach((node) => {
    if (!levels.has(node.step)) {
      levels.set(node.step, 0);
    }
  });

  const nodesByLevel = new Map<number, PipelineNode[]>();
  pipeline.nodes.forEach((node) => {
    const level = levels.get(node.step) || 0;
    const bucket = nodesByLevel.get(level) || [];
    bucket.push(node);
    nodesByLevel.set(level, bucket);
  });

  const levelEntries = [...nodesByLevel.entries()].sort((left, right) => left[0] - right[0]);
  levelEntries.forEach(([, nodes]) => nodes.sort((left, right) => left.step - right.step));

  const positionedNodes: PositionedNode[] = [];
  levelEntries.forEach(([level, nodes]) => {
    nodes.forEach((node, rowIndex) => {
      positionedNodes.push({
        node,
        x: PADDING_X + level * (CARD_WIDTH + COLUMN_GAP),
        y: PADDING_Y + rowIndex * (CARD_HEIGHT + ROW_GAP),
      });
    });
  });

  const positionByStep = new Map<number, PositionedNode>();
  positionedNodes.forEach((positionedNode) => {
    positionByStep.set(positionedNode.node.step, positionedNode);
  });

  const drawableEdges: DrawableEdge[] = pipeline.edges
    .map((edge) => {
      const from = positionByStep.get(edge.from_step);
      const to = positionByStep.get(edge.to_step);
      if (!from || !to) {
        return null;
      }

      const startX = from.x + CARD_WIDTH;
      const startY = from.y + CARD_HEIGHT / 2;
      const endX = to.x;
      const endY = to.y + CARD_HEIGHT / 2;
      const controlOffset = Math.max(48, (endX - startX) / 2);
      const path = [
        `M ${startX} ${startY}`,
        `C ${startX + controlOffset} ${startY}, ${endX - controlOffset} ${endY}, ${endX} ${endY}`,
      ].join(' ');

      return {
        edge,
        path,
        labelX: startX + (endX - startX) / 2,
        labelY: startY + (endY - startY) / 2 - 12,
      };
    })
    .filter((edge): edge is DrawableEdge => edge !== null);

  const maxLevel = Math.max(...levelEntries.map(([level]) => level), 0);
  const maxRows = Math.max(...levelEntries.map(([, nodes]) => nodes.length), 1);

  return {
    positionedNodes,
    drawableEdges,
    width: PADDING_X * 2 + (maxLevel + 1) * CARD_WIDTH + maxLevel * COLUMN_GAP,
    height: PADDING_Y * 2 + maxRows * CARD_HEIGHT + Math.max(0, maxRows - 1) * ROW_GAP,
  };
};

export const Pipelines: React.FC = () => {
  const location = useLocation();
  const { currentPipeline } = usePipelineContext();
  const initialMessage = location.state?.initialMessage;
  const dialogId = location.state?.dialogId;
  const graphLayout = currentPipeline && currentPipeline.nodes.length > 0
    ? buildGraphLayout(currentPipeline)
    : null;

  return (
    <div className="h-full flex overflow-hidden">
      {/* Main Pipeline Zone - Center */}
      <div className="flex-1 relative bg-muted/5 bg-grid-pattern p-8 overflow-auto">
        <div className="space-y-12 py-10 min-w-max">
          <div className="flex flex-col items-center mb-12">
            <h1 className="text-2xl font-bold text-foreground mb-2">Editor Pipeline</h1>
            <p className="text-sm text-muted-foreground">Визуализация текущего процесса автоматизации</p>
          </div>

          {graphLayout && currentPipeline ? (
            <div
              className="relative mx-auto"
              style={{
                width: graphLayout.width,
                height: graphLayout.height,
              }}
            >
              <svg
                className="absolute inset-0 h-full w-full"
                viewBox={`0 0 ${graphLayout.width} ${graphLayout.height}`}
                fill="none"
              >
                {graphLayout.drawableEdges.map(({ edge, path, labelX, labelY }) => (
                  <g key={`${edge.from_step}-${edge.to_step}-${edge.type}`}>
                    <path
                      d={path}
                      className="stroke-primary/50"
                      strokeWidth="2.5"
                      strokeLinecap="round"
                    />
                    <circle
                      cx={labelX}
                      cy={labelY + 12}
                      r="4"
                      className="fill-primary/80"
                    />
                    <text
                      x={labelX}
                      y={labelY}
                      textAnchor="middle"
                      className="fill-muted-foreground text-[11px]"
                    >
                      {edge.type}
                    </text>
                  </g>
                ))}
              </svg>

              {graphLayout.positionedNodes.map(({ node, x, y }) => {
                const endpoint = node.endpoints[0];
                return (
                  <Card
                    key={node.step}
                    className="absolute z-10 border-primary/20 bg-card/95 shadow-lg"
                    style={{
                      width: CARD_WIDTH,
                      minHeight: CARD_HEIGHT,
                      left: x,
                      top: y,
                    }}
                  >
                    <div className="flex h-full flex-col gap-4 p-5">
                      <div className="flex items-start justify-between gap-3">
                        <div>
                          <p className="text-xs font-medium uppercase tracking-[0.18em] text-primary/70">
                            Step {node.step}
                          </p>
                          <h3 className="mt-1 text-base font-semibold text-foreground">{node.name}</h3>
                        </div>
                        <Badge variant="outline" className="bg-primary/10 text-primary border-primary/20">
                          API
                        </Badge>
                      </div>

                      <p className="text-sm leading-6 text-muted-foreground">
                        {node.description || 'Описание шага не указано.'}
                      </p>

                      <div className="space-y-2 text-xs text-muted-foreground">
                        <div>
                          <span className="font-semibold text-foreground">Capability:</span>{' '}
                          {endpoint?.name || 'Не определено'}
                        </div>
                        <div>
                          <span className="font-semibold text-foreground">Inputs:</span>{' '}
                          {node.input_connected_from.length > 0 ? `from ${node.input_connected_from.join(', ')}` : 'external'}
                        </div>
                        <div>
                          <span className="font-semibold text-foreground">Outputs:</span>{' '}
                          {node.output_connected_to.length > 0 ? node.output_connected_to.join(', ') : 'terminal'}
                        </div>
                      </div>

                      {node.external_inputs.length > 0 && (
                        <div className="flex flex-wrap gap-2">
                          {node.external_inputs.map((inputName) => (
                            <Badge
                              key={inputName}
                              variant="outline"
                              className={cn(
                                "border-amber-500/30 bg-amber-500/10 text-amber-700"
                              )}
                            >
                              {inputName}
                            </Badge>
                          ))}
                        </div>
                      )}
                    </div>
                  </Card>
                );
              })}
            </div>
          ) : (
            <Card className="mx-auto flex max-w-2xl items-center gap-4 border-dashed border-primary/20 bg-card/70 p-8">
              <div className="flex h-12 w-12 items-center justify-center rounded-xl bg-primary/10 text-primary">
                <Sparkles className="h-6 w-6" />
              </div>
              <div>
                <p className="font-semibold text-foreground">Pipeline еще не построен</p>
                <p className="text-sm text-muted-foreground">
                  Отправьте сообщение в чат справа после импорта OpenAPI, и граф появится здесь.
                </p>
              </div>
            </Card>
          )}

          <Card className="mt-20 p-6 bg-primary/5 border-dashed border-primary/20 flex items-center justify-between">
            <div className="flex items-center gap-4">
              <div className="h-12 w-12 rounded-xl bg-primary/10 flex items-center justify-center text-primary">
                <Activity className="h-6 w-6" />
              </div>
              <div>
                <p className="font-semibold text-foreground">Статус пайплайна</p>
                <p className="text-sm text-muted-foreground">
                  {currentPipeline?.message_ru || 'Все модули в режиме ожидания запуска'}
                </p>
              </div>
            </div>
            <Button className="gap-2" disabled={!currentPipeline?.pipeline_id}>
              <Play className="h-4 w-4" /> Запустить поток
            </Button>
          </Card>
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
