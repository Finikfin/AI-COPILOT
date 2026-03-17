import React from 'react';
import { useLocation } from 'react-router-dom';
import ReactFlow, {
  BaseEdge,
  Background,
  Connection,
  Controls,
  Edge,
  EdgeLabelRenderer,
  EdgeProps,
  Handle,
  MarkerType,
  Node,
  NodeChange,
  NodeProps,
  Position,
  ReactFlowInstance,
  applyNodeChanges,
  getBezierPath,
} from 'reactflow';
import 'reactflow/dist/style.css';
import { SynthesisChat } from '@/components/shared/SynthesisChat';
import { Card } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Input } from '@/components/ui/input';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import {
  Activity,
  Download,
  Play,
  MessageSquare,
  Sparkles,
  Loader2,
  X,
  Trash2,
  Plus,
  Copy,
  Check,
} from 'lucide-react';
import { usePipelineContext } from '@/contexts/PipelineContext';
import { motion, AnimatePresence } from 'framer-motion';
import { PipelineData, PipelineEdge, PipelineNode } from '@/types/pipeline';
import { cn } from '@/lib/utils';
import {
  ExecutionHttpMethod,
  ExecutionRunDetailResponse,
  ExecutionRunStatus,
  ExecutionStepRunResponse,
  ExecutionStepStatus,
  getExecution,
  runPipeline,
} from '@/api/executions';
import { updatePipelineGraph } from '@/api/pipelines';
import { toast } from 'sonner';

type PipelineCanvasNodeData = {
  node: PipelineNode;
  stepStatus: ExecutionStepStatus | null;
};

type PipelineCanvasEdgeData = {
  edgeType: string;
};

type EdgeDialogMode = 'create' | 'edit';

type EdgeDialogState = {
  open: boolean;
  mode: EdgeDialogMode;
  sourceStep: number;
  targetStep: number;
  edgeId: string | null;
  value: string;
  suggestions: string[];
};

const NODE_WIDTH = 260;
const NODE_HEIGHT = 104;
const COLUMN_GAP = 130;
const ROW_GAP = 72;
const PADDING_X = 48;
const PADDING_Y = 32;
const NEW_NODE_DRAG_TYPE = 'application/x-pipeline-node';

const TERMINAL_RUN_STATUSES: ExecutionRunStatus[] = [
  'SUCCEEDED',
  'FAILED',
  'PARTIAL_FAILED',
];

const REQUEST_BODY_METHODS: ExecutionHttpMethod[] = ['POST', 'PUT', 'PATCH'];

const emptyEdgeDialogState: EdgeDialogState = {
  open: false,
  mode: 'create',
  sourceStep: 0,
  targetStep: 0,
  edgeId: null,
  value: '',
  suggestions: [],
};

const isTerminalRunStatus = (status: ExecutionRunStatus) =>
  TERMINAL_RUN_STATUSES.includes(status);

const getRunStatusMeta = (status: ExecutionRunStatus | null) => {
  if (status === 'SUCCEEDED') {
    return {
      label: 'SUCCEEDED',
      badgeClass: 'border-emerald-500/40 bg-emerald-500/10 text-emerald-700',
    };
  }
  if (status === 'FAILED') {
    return {
      label: 'FAILED',
      badgeClass: 'border-red-500/40 bg-red-500/10 text-red-700',
    };
  }
  if (status === 'PARTIAL_FAILED') {
    return {
      label: 'PARTIAL_FAILED',
      badgeClass: 'border-amber-500/40 bg-amber-500/10 text-amber-700',
    };
  }
  if (status === 'RUNNING') {
    return {
      label: 'RUNNING',
      badgeClass: 'border-blue-500/40 bg-blue-500/10 text-blue-700',
    };
  }
  if (status === 'QUEUED') {
    return {
      label: 'QUEUED',
      badgeClass: 'border-slate-500/40 bg-slate-500/10 text-slate-700',
    };
  }
  return {
    label: 'IDLE',
    badgeClass: 'border-border bg-muted text-muted-foreground',
  };
};

const getStepStatusMeta = (status: ExecutionStepStatus | undefined | null) => {
  if (status === 'SUCCEEDED') {
    return {
      label: 'SUCCEEDED',
      cardClass: 'border-emerald-500/40 bg-emerald-500/10',
      badgeClass: 'border-emerald-500/40 bg-emerald-500/10 text-emerald-700',
    };
  }
  if (status === 'FAILED') {
    return {
      label: 'FAILED',
      cardClass: 'border-red-500/40 bg-red-500/10',
      badgeClass: 'border-red-500/40 bg-red-500/10 text-red-700',
    };
  }
  if (status === 'RUNNING') {
    return {
      label: 'RUNNING',
      cardClass: 'border-blue-500/40 bg-blue-500/10 ring-1 ring-blue-500/20',
      badgeClass: 'border-blue-500/40 bg-blue-500/10 text-blue-700',
    };
  }
  if (status === 'SKIPPED') {
    return {
      label: 'SKIPPED',
      cardClass: 'border-amber-500/40 bg-amber-500/10',
      badgeClass: 'border-amber-500/40 bg-amber-500/10 text-amber-700',
    };
  }
  return {
    label: 'PENDING',
    cardClass: '',
    badgeClass: 'border-border bg-muted text-muted-foreground',
  };
};

export const hasRequestBody = (
  method: ExecutionHttpMethod | null | undefined
) => Boolean(method && REQUEST_BODY_METHODS.includes(method));

export const formatPayload = (payload: unknown): string => {
  if (payload === null || payload === undefined) {
    return 'нет данных';
  }
  if (typeof payload === 'string') {
    return payload || 'нет данных';
  }
  try {
    return JSON.stringify(payload, null, 2);
  } catch {
    return String(payload);
  }
};

type PayloadView = {
  raw: string;
  display: string;
  kind: 'json' | 'text';
};

const buildPayloadView = (payload: unknown): PayloadView => {
  if (payload === null || payload === undefined) {
    return {
      raw: 'нет данных',
      display: 'нет данных',
      kind: 'text',
    };
  }

  if (typeof payload === 'string') {
    const trimmed = payload.trim();
    if (!trimmed) {
      return {
        raw: 'нет данных',
        display: 'нет данных',
        kind: 'text',
      };
    }

    try {
      const parsed = JSON.parse(trimmed);
      const pretty = JSON.stringify(parsed, null, 2);
      return {
        raw: pretty,
        display: pretty,
        kind: 'json',
      };
    } catch {
      return {
        raw: payload,
        display: payload,
        kind: 'text',
      };
    }
  }

  try {
    const pretty = JSON.stringify(payload, null, 2);
    return {
      raw: pretty,
      display: pretty,
      kind: 'json',
    };
  } catch {
    const fallback = String(payload);
    return {
      raw: fallback,
      display: fallback,
      kind: 'text',
    };
  }
};

const PayloadViewer: React.FC<{ payload: unknown }> = ({ payload }) => {
  const [copied, setCopied] = React.useState(false);
  const view = React.useMemo(() => buildPayloadView(payload), [payload]);

  const handleCopy = React.useCallback(async () => {
    try {
      await navigator.clipboard.writeText(view.raw);
      setCopied(true);
      window.setTimeout(() => setCopied(false), 1200);
    } catch {
      toast.error('Не удалось скопировать');
    }
  }, [view.raw]);

  return (
    <div className="rounded-xl border border-border/60 bg-gradient-to-b from-zinc-50/80 to-zinc-100/30 p-2 dark:from-zinc-900/60 dark:to-zinc-900/20">
      <div className="mb-2 flex items-center justify-between gap-2 px-1">
        <Badge
          variant="outline"
          className="h-5 border-zinc-300/70 bg-white/80 px-2 text-[10px] font-semibold uppercase tracking-wide text-zinc-700"
        >
          {view.kind}
        </Badge>
        <Button
          variant="ghost"
          size="sm"
          className="h-6 gap-1 px-2 text-[10px] text-zinc-600 hover:bg-zinc-200/60"
          onClick={handleCopy}
        >
          {copied ? <Check className="h-3 w-3" /> : <Copy className="h-3 w-3" />}
          {copied ? 'Copied' : 'Copy'}
        </Button>
      </div>
      <pre className="max-h-48 overflow-auto rounded-lg border border-zinc-200/70 bg-white/70 p-3 text-[11px] leading-relaxed text-zinc-700">
        {view.display}
      </pre>
    </div>
  );
};

const buildLayoutStorageKey = (pipelineId: string) =>
  `pipeline_layout:${pipelineId}:v1`;

const edgeIdFrom = (edge: PipelineEdge, index: number) =>
  `edge:${edge.from_step}:${edge.to_step}:${encodeURIComponent(edge.type)}:${index}`;

const toPipelineEdges = (
  edges: Array<Edge<PipelineCanvasEdgeData>>
): PipelineEdge[] => {
  return edges
    .map((edge) => {
      const fromStep = Number(edge.source);
      const toStep = Number(edge.target);
      const type = String(edge.data?.edgeType ?? edge.label ?? '').trim();

      if (!Number.isInteger(fromStep) || !Number.isInteger(toStep) || !type) {
        return null;
      }

      return {
        from_step: fromStep,
        to_step: toStep,
        type,
      };
    })
    .filter((item): item is PipelineEdge => item !== null);
};

const syncNodesWithEdges = (
  nodes: PipelineNode[],
  edges: PipelineEdge[]
): PipelineNode[] => {
  const incomingByStep = new Map<number, Set<number>>();
  const outgoingByStep = new Map<number, Set<number>>();
  const typedIncoming = new Map<number, Set<string>>();

  nodes.forEach((node) => {
    incomingByStep.set(node.step, new Set());
    outgoingByStep.set(node.step, new Set());
    typedIncoming.set(node.step, new Set());
  });

  edges.forEach((edge) => {
    if (!incomingByStep.has(edge.to_step) || !outgoingByStep.has(edge.from_step)) {
      return;
    }
    incomingByStep.get(edge.to_step)?.add(edge.from_step);
    outgoingByStep.get(edge.from_step)?.add(edge.to_step);
    typedIncoming.get(edge.to_step)?.add(`${edge.from_step}::${edge.type}`);
  });

  return nodes.map((node) => {
    const typed = [...(typedIncoming.get(node.step) || new Set())]
      .map((item) => {
        const [fromStepRaw, type] = item.split('::');
        const fromStep = Number(fromStepRaw);
        if (!Number.isInteger(fromStep) || !type) {
          return null;
        }
        return { from_step: fromStep, type };
      })
      .filter((item): item is { from_step: number; type: string } => item !== null)
      .sort((left, right) => {
        if (left.from_step !== right.from_step) {
          return left.from_step - right.from_step;
        }
        return left.type.localeCompare(right.type);
      });

    return {
      ...node,
      input_connected_from: [...(incomingByStep.get(node.step) || new Set())].sort(
        (left, right) => left - right
      ),
      output_connected_to: [...(outgoingByStep.get(node.step) || new Set())].sort(
        (left, right) => left - right
      ),
      input_data_type_from_previous: typed,
    };
  });
};

const buildAutoLayout = (nodes: PipelineNode[], edges: PipelineEdge[]) => {
  const nodeByStep = new Map<number, PipelineNode>();
  const incomingCount = new Map<number, number>();
  const outgoing = new Map<number, number[]>();

  nodes.forEach((node) => {
    nodeByStep.set(node.step, node);
    incomingCount.set(node.step, 0);
    outgoing.set(node.step, []);
  });

  edges.forEach((edge) => {
    if (!nodeByStep.has(edge.from_step) || !nodeByStep.has(edge.to_step)) {
      return;
    }
    outgoing.get(edge.from_step)?.push(edge.to_step);
    incomingCount.set(edge.to_step, (incomingCount.get(edge.to_step) || 0) + 1);
  });

  const roots = nodes
    .filter((node) => (incomingCount.get(node.step) || 0) === 0)
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
    (outgoing.get(current.step) || []).forEach((childStep) => {
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

  nodes.forEach((node) => {
    if (!levels.has(node.step)) {
      levels.set(node.step, 0);
    }
  });

  const grouped = new Map<number, PipelineNode[]>();
  nodes.forEach((node) => {
    const level = levels.get(node.step) || 0;
    const bucket = grouped.get(level) || [];
    bucket.push(node);
    grouped.set(level, bucket);
  });

  const positions: Record<number, { x: number; y: number }> = {};
  [...grouped.entries()]
    .sort((left, right) => left[0] - right[0])
    .forEach(([level, levelNodes]) => {
      levelNodes.sort((left, right) => left.step - right.step);
      levelNodes.forEach((node, row) => {
        positions[node.step] = {
          x: PADDING_X + level * (NODE_WIDTH + COLUMN_GAP),
          y: PADDING_Y + row * (NODE_HEIGHT + ROW_GAP),
        };
      });
    });

  return positions;
};

const parseLayout = (raw: string | null): Record<number, { x: number; y: number }> => {
  if (!raw) {
    return {};
  }

  try {
    const parsed = JSON.parse(raw) as Record<string, { x?: unknown; y?: unknown }>;
    return Object.entries(parsed).reduce<Record<number, { x: number; y: number }>>(
      (acc, [stepRaw, point]) => {
        const step = Number(stepRaw);
        const x = Number(point?.x);
        const y = Number(point?.y);
        if (!Number.isInteger(step) || Number.isNaN(x) || Number.isNaN(y)) {
          return acc;
        }
        acc[step] = { x, y };
        return acc;
      },
      {}
    );
  } catch {
    return {};
  }
};

const createDraftPipelineNode = (step: number): PipelineNode => ({
  step,
  name: `Custom Step ${step}`,
  description: 'Новый шаг (перетащите и настройте связи)',
  input_connected_from: [],
  output_connected_to: [],
  input_data_type_from_previous: [],
  external_inputs: [],
  endpoints: [],
});

const getNextStepNumber = (nodes: Array<Node<PipelineCanvasNodeData>>): number => {
  const maxStep = nodes.reduce((max, node) => {
    const step = Number(node.id);
    if (!Number.isInteger(step)) {
      return max;
    }
    return Math.max(max, step);
  }, 0);
  return maxStep + 1;
};

const resolveDropPosition = (
  reactFlowInstance: ReactFlowInstance,
  event: React.DragEvent,
  wrapperBounds: DOMRect
) => {
  const instance = reactFlowInstance as ReactFlowInstance & {
    screenToFlowPosition?: (position: { x: number; y: number }) => { x: number; y: number };
    project?: (position: { x: number; y: number }) => { x: number; y: number };
  };

  if (typeof instance.screenToFlowPosition === 'function') {
    return instance.screenToFlowPosition({
      x: event.clientX,
      y: event.clientY,
    });
  }

  if (typeof instance.project === 'function') {
    return instance.project({
      x: event.clientX - wrapperBounds.left,
      y: event.clientY - wrapperBounds.top,
    });
  }

  return {
    x: event.clientX - wrapperBounds.left,
    y: event.clientY - wrapperBounds.top,
  };
};

const PipelineFlowEdge = ({
  id,
  sourceX,
  sourceY,
  targetX,
  targetY,
  sourcePosition,
  targetPosition,
  markerEnd,
  data,
  label,
}: EdgeProps<PipelineCanvasEdgeData>) => {
  const [edgePath, labelX, labelY] = getBezierPath({
    sourceX,
    sourceY,
    targetX,
    targetY,
    sourcePosition,
    targetPosition,
  });
  const edgeType = String(data?.edgeType ?? label ?? '').trim();

  return (
    <>
      <BaseEdge
        id={id}
        path={edgePath}
        markerEnd={markerEnd}
        style={{
          stroke: '#22c55e',
          strokeWidth: 3,
        }}
      />
      <EdgeLabelRenderer>
        <div
          style={{
            position: 'absolute',
            transform: `translate(-50%, -50%) translate(${labelX}px,${labelY}px)`,
            pointerEvents: 'none',
          }}
        >
          <div className="relative flex items-center justify-center">
            {edgeType ? (
              <span className="absolute -top-5 whitespace-nowrap text-[11px] font-semibold text-zinc-200">
                {edgeType}
              </span>
            ) : null}
            <span className="h-3.5 w-3.5 rounded-full bg-emerald-500 shadow-[0_0_0_4px_rgba(34,197,94,0.18)]" />
          </div>
        </div>
      </EdgeLabelRenderer>
    </>
  );
};

const toCanvasEdge = (
  edge: PipelineEdge,
  index: number
): Edge<PipelineCanvasEdgeData> => ({
  id: edgeIdFrom(edge, index),
  source: String(edge.from_step),
  target: String(edge.to_step),
  type: 'pipelineFlowEdge',
  label: edge.type,
  data: {
    edgeType: edge.type,
  },
  markerEnd: {
    type: MarkerType.ArrowClosed,
    width: 18,
    height: 18,
    color: '#22c55e',
  },
  style: {
    stroke: '#22c55e',
    strokeWidth: 3,
  },
  labelStyle: {
    fontSize: 11,
  },
});

const extractTypeHints = (value: unknown): string[] => {
  if (!value) {
    return [];
  }

  if (typeof value === 'string') {
    const trimmed = value.trim();
    return trimmed ? [trimmed] : [];
  }

  if (Array.isArray(value)) {
    return value
      .filter((item): item is string => typeof item === 'string')
      .map((item) => item.trim())
      .filter(Boolean);
  }

  if (typeof value !== 'object') {
    return [];
  }

  const typed = value as {
    required?: unknown;
    properties?: unknown;
  };

  const hints = new Set<string>();
  if (Array.isArray(typed.required)) {
    typed.required.forEach((field) => {
      if (typeof field === 'string' && field.trim()) {
        hints.add(field.trim());
      }
    });
  }

  if (typed.properties && typeof typed.properties === 'object') {
    Object.keys(typed.properties).forEach((key) => {
      if (key.trim()) {
        hints.add(key.trim());
      }
    });
  }

  if (hints.size > 0) {
    return [...hints];
  }

  Object.keys(value as Record<string, unknown>).forEach((key) => {
    if (key.trim()) {
      hints.add(key.trim());
    }
  });

  return [...hints];
};

const collectEdgeTypeSuggestions = (
  sourceNode: PipelineNode | null,
  targetNode: PipelineNode | null,
  existingEdges: PipelineEdge[]
): string[] => {
  const hints = new Set<string>();

  sourceNode?.endpoints.forEach((endpoint) => {
    extractTypeHints(endpoint.output_type).forEach((hint) => hints.add(hint));
  });

  targetNode?.endpoints.forEach((endpoint) => {
    extractTypeHints(endpoint.input_type).forEach((hint) => hints.add(hint));
  });

  targetNode?.external_inputs.forEach((inputName) => {
    if (inputName.trim()) {
      hints.add(inputName.trim());
    }
  });

  existingEdges.forEach((edge) => {
    if (edge.type.trim()) {
      hints.add(edge.type.trim());
    }
  });

  return [...hints];
};

const wouldCreateCycle = (
  edges: PipelineEdge[],
  sourceStep: number,
  targetStep: number
): boolean => {
  if (sourceStep === targetStep) {
    return true;
  }

  const adjacency = new Map<number, Set<number>>();
  edges.forEach((edge) => {
    const next = adjacency.get(edge.from_step) || new Set<number>();
    next.add(edge.to_step);
    adjacency.set(edge.from_step, next);
  });

  const sourceChildren = adjacency.get(sourceStep) || new Set<number>();
  sourceChildren.add(targetStep);
  adjacency.set(sourceStep, sourceChildren);

  const stack = [targetStep];
  const visited = new Set<number>();

  while (stack.length > 0) {
    const current = stack.pop();
    if (current === undefined) {
      continue;
    }
    if (current === sourceStep) {
      return true;
    }
    if (visited.has(current)) {
      continue;
    }
    visited.add(current);
    (adjacency.get(current) || []).forEach((next) => stack.push(next));
  }

  return false;
};

const PipelineStepNode = React.memo(
  ({ data, selected }: NodeProps<PipelineCanvasNodeData>) => {
    const statusMeta = getStepStatusMeta(data.stepStatus);
    return (
      <div
        className={cn(
          'w-[260px] rounded-xl border bg-card/95 px-4 py-3 shadow-md backdrop-blur',
          selected && 'ring-2 ring-primary/25',
          statusMeta.cardClass
        )}
      >
        <Handle
          type="target"
          position={Position.Left}
          className="h-3 w-3 border border-primary/50 bg-background"
        />
        <Handle
          type="source"
          position={Position.Right}
          className="h-3 w-3 border border-primary/50 bg-background"
        />
        <div className="flex items-start justify-between gap-2">
          <div className="min-w-0">
            <p className="text-[10px] font-bold uppercase tracking-[0.12em] text-primary/70">
              Step {data.node.step}
            </p>
            <p className="mt-1 truncate text-sm font-semibold text-foreground">
              {data.node.name}
            </p>
          </div>
          <Badge variant="outline" className={cn('text-[10px]', statusMeta.badgeClass)}>
            {statusMeta.label}
          </Badge>
        </div>
        <p className="mt-2 line-clamp-2 text-[11px] leading-relaxed text-muted-foreground">
          {data.node.description || 'Описание шага не указано.'}
        </p>
      </div>
    );
  }
);

PipelineStepNode.displayName = 'PipelineStepNode';

const nodeTypes = {
  pipelineStep: PipelineStepNode,
};

const edgeTypes = {
  pipelineFlowEdge: PipelineFlowEdge,
};

export const Pipelines: React.FC = () => {
  const location = useLocation();
  const { currentPipeline, isHydrating, setPipeline } = usePipelineContext();
  const [expandedStep, setExpandedStep] = React.useState<number | null>(null);
  const [execution, setExecution] = React.useState<ExecutionRunDetailResponse | null>(
    null
  );
  const [activeRunId, setActiveRunId] = React.useState<string | null>(null);
  const [isRunStarting, setIsRunStarting] = React.useState(false);
  const [isChatVisible, setIsChatVisible] = React.useState(() => {
    const saved = localStorage.getItem('pipelines_chat_visible');
    return saved !== null ? saved === 'true' : true;
  });

  const [canvasNodes, setCanvasNodes] = React.useState<
    Array<Node<PipelineCanvasNodeData>>
  >([]);
  const [canvasEdges, setCanvasEdges] = React.useState<
    Array<Edge<PipelineCanvasEdgeData>>
  >([]);
  const [edgeDialog, setEdgeDialog] = React.useState<EdgeDialogState>(
    emptyEdgeDialogState
  );
  const [hasUnsavedGraphChanges, setHasUnsavedGraphChanges] = React.useState(false);
  const [isGraphSaveInFlight, setIsGraphSaveInFlight] = React.useState(false);
  const [graphRevision, setGraphRevision] = React.useState(0);
  const reactFlowWrapperRef = React.useRef<HTMLDivElement | null>(null);
  const [reactFlowInstance, setReactFlowInstance] =
    React.useState<ReactFlowInstance | null>(null);

  const graphRevisionRef = React.useRef(0);
  const lastSavedGraphRef = React.useRef<{
    pipelineId: string;
    nodes: PipelineNode[];
    edges: PipelineEdge[];
  } | null>(null);
  const latestPipelineRef = React.useRef<PipelineData | null>(currentPipeline);

  const pollingTimerRef = React.useRef<number | null>(null);
  const isPollingRequestInFlightRef = React.useRef(false);
  const notifiedTerminalStatusRef = React.useRef<ExecutionRunStatus | null>(null);
  const lastPipelineIdRef = React.useRef<string | null>(null);

  React.useEffect(() => {
    localStorage.setItem('pipelines_chat_visible', String(isChatVisible));
  }, [isChatVisible]);

  React.useEffect(() => {
    latestPipelineRef.current = currentPipeline;
  }, [currentPipeline]);

  React.useEffect(() => {
    graphRevisionRef.current = graphRevision;
  }, [graphRevision]);

  const initialMessage = location.state?.initialMessage;
  const dialogId = location.state?.dialogId;
  const pipelineId = currentPipeline?.pipeline_id || null;

  const finalOutput = React.useMemo(
    () => execution?.summary?.final_output,
    [execution]
  );

  const stepRunsByStep = React.useMemo(() => {
    const byStep = new Map<number, ExecutionStepRunResponse>();
    execution?.steps.forEach((stepRun) => {
      byStep.set(stepRun.step, stepRun);
    });
    return byStep;
  }, [execution]);

  const runStatusMeta = getRunStatusMeta(execution?.status || null);
  const isExecutionInProgress = execution
    ? !isTerminalRunStatus(execution.status)
    : Boolean(activeRunId);

  const isGraphSavePending = hasUnsavedGraphChanges || isGraphSaveInFlight;

  const selectedNode = React.useMemo(() => {
    if (expandedStep === null) {
      return null;
    }
    return canvasNodes.find((node) => Number(node.id) === expandedStep)?.data.node || null;
  }, [canvasNodes, expandedStep]);

  const selectedStepRun = React.useMemo(() => {
    if (expandedStep === null) {
      return null;
    }
    return stepRunsByStep.get(expandedStep) || null;
  }, [expandedStep, stepRunsByStep]);

  const markGraphAsDirty = React.useCallback(() => {
    setHasUnsavedGraphChanges(true);
    setGraphRevision((prev) => prev + 1);
  }, []);

  const syncCanvasNodesConnections = React.useCallback(
    (nextEdges: Array<Edge<PipelineCanvasEdgeData>>) => {
      const pipelineEdges = toPipelineEdges(nextEdges);
      setCanvasNodes((prevNodes) => {
        const currentPipelineNodes = prevNodes.map((node) => node.data.node);
        const synced = syncNodesWithEdges(currentPipelineNodes, pipelineEdges);
        const byStep = new Map<number, PipelineNode>();
        synced.forEach((node) => byStep.set(node.step, node));

        return prevNodes.map((canvasNode) => {
          const step = Number(canvasNode.id);
          const syncedNode = byStep.get(step);
          if (!syncedNode) {
            return canvasNode;
          }
          return {
            ...canvasNode,
            data: {
              ...canvasNode.data,
              node: syncedNode,
            },
          };
        });
      });
    },
    []
  );

  const closeEdgeDialog = React.useCallback(() => {
    setEdgeDialog(emptyEdgeDialogState);
  }, []);

  const openCreateEdgeDialog = React.useCallback(
    (sourceStep: number, targetStep: number) => {
      const pipelineEdges = toPipelineEdges(canvasEdges);
      const sourceNode = canvasNodes.find((node) => Number(node.id) === sourceStep)?.data
        .node;
      const targetNode = canvasNodes.find((node) => Number(node.id) === targetStep)?.data
        .node;

      setEdgeDialog({
        open: true,
        mode: 'create',
        sourceStep,
        targetStep,
        edgeId: null,
        value: '',
        suggestions: collectEdgeTypeSuggestions(
          sourceNode || null,
          targetNode || null,
          pipelineEdges
        ),
      });
    },
    [canvasEdges, canvasNodes]
  );

  const openEditEdgeDialog = React.useCallback(
    (edge: Edge<PipelineCanvasEdgeData>) => {
      const sourceStep = Number(edge.source);
      const targetStep = Number(edge.target);
      if (!Number.isInteger(sourceStep) || !Number.isInteger(targetStep)) {
        return;
      }

      const pipelineEdges = toPipelineEdges(canvasEdges);
      const sourceNode = canvasNodes.find((node) => Number(node.id) === sourceStep)?.data
        .node;
      const targetNode = canvasNodes.find((node) => Number(node.id) === targetStep)?.data
        .node;

      setEdgeDialog({
        open: true,
        mode: 'edit',
        sourceStep,
        targetStep,
        edgeId: edge.id,
        value: String(edge.data?.edgeType ?? edge.label ?? '').trim(),
        suggestions: collectEdgeTypeSuggestions(
          sourceNode || null,
          targetNode || null,
          pipelineEdges
        ),
      });
    },
    [canvasEdges, canvasNodes]
  );

  const handleConfirmEdgeDialog = React.useCallback(() => {
    const nextType = edgeDialog.value.trim();

    if (edgeDialog.mode === 'create') {
      if (!nextType) {
        closeEdgeDialog();
        return;
      }

      const existing = toPipelineEdges(canvasEdges);
      const duplicate = existing.some(
        (edge) =>
          edge.from_step === edgeDialog.sourceStep &&
          edge.to_step === edgeDialog.targetStep &&
          edge.type === nextType
      );
      if (duplicate) {
        toast.error('Связь с таким type уже существует');
        return;
      }

      const nextEdges = [
        ...canvasEdges,
        toCanvasEdge(
          {
            from_step: edgeDialog.sourceStep,
            to_step: edgeDialog.targetStep,
            type: nextType,
          },
          canvasEdges.length
        ),
      ];

      setCanvasEdges(nextEdges);
      syncCanvasNodesConnections(nextEdges);
      markGraphAsDirty();
      closeEdgeDialog();
      return;
    }

    if (!edgeDialog.edgeId) {
      closeEdgeDialog();
      return;
    }

    if (!nextType) {
      toast.error('Type связи не может быть пустым');
      return;
    }

    const duplicate = toPipelineEdges(canvasEdges).some((edge, index) => {
      const id = canvasEdges[index]?.id;
      if (id === edgeDialog.edgeId) {
        return false;
      }
      return (
        edge.from_step === edgeDialog.sourceStep &&
        edge.to_step === edgeDialog.targetStep &&
        edge.type === nextType
      );
    });

    if (duplicate) {
      toast.error('Связь с таким type уже существует');
      return;
    }

    const nextEdges = canvasEdges.map((edge) => {
      if (edge.id !== edgeDialog.edgeId) {
        return edge;
      }
      return {
        ...edge,
        label: nextType,
        data: {
          edgeType: nextType,
        },
      };
    });

    setCanvasEdges(nextEdges);
    syncCanvasNodesConnections(nextEdges);
    markGraphAsDirty();
    closeEdgeDialog();
  }, [
    canvasEdges,
    closeEdgeDialog,
    edgeDialog,
    markGraphAsDirty,
    syncCanvasNodesConnections,
  ]);

  const deleteEdgeById = React.useCallback(
    (edgeId: string) => {
      const nextEdges = canvasEdges.filter((edge) => edge.id !== edgeId);
      if (nextEdges.length === canvasEdges.length) {
        return;
      }
      setCanvasEdges(nextEdges);
      syncCanvasNodesConnections(nextEdges);
      markGraphAsDirty();
      closeEdgeDialog();
    },
    [canvasEdges, closeEdgeDialog, markGraphAsDirty, syncCanvasNodesConnections]
  );

  const stopPollingExecution = React.useCallback(() => {
    if (pollingTimerRef.current !== null) {
      window.clearInterval(pollingTimerRef.current);
      pollingTimerRef.current = null;
    }
    isPollingRequestInFlightRef.current = false;
  }, []);

  const pollExecution = React.useCallback(
    async (runId: string, options?: { silent?: boolean }) => {
      const silent = options?.silent ?? false;
      if (isPollingRequestInFlightRef.current) {
        return;
      }
      isPollingRequestInFlightRef.current = true;

      try {
        const runDetail = await getExecution(runId);
        setExecution(runDetail);
        setActiveRunId(runId);

        if (!isTerminalRunStatus(runDetail.status)) {
          notifiedTerminalStatusRef.current = null;
          return;
        }

        stopPollingExecution();
        const isNewTerminalStatus = notifiedTerminalStatusRef.current !== runDetail.status;
        if (!silent && isNewTerminalStatus) {
          if (runDetail.status === 'SUCCEEDED') {
            toast.success('Пайплайн выполнен успешно');
          } else {
            toast.error(runDetail.error || 'Пайплайн завершился с ошибками');
          }
        }
        notifiedTerminalStatusRef.current = runDetail.status;
      } catch (error) {
        stopPollingExecution();
        if (!silent) {
          toast.error(
            error instanceof Error ? error.message : 'Не удалось получить статус выполнения'
          );
        }
      } finally {
        isPollingRequestInFlightRef.current = false;
      }
    },
    [stopPollingExecution]
  );

  const startPollingExecution = React.useCallback(
    (runId: string) => {
      stopPollingExecution();
      pollExecution(runId).catch(() => null);
      pollingTimerRef.current = window.setInterval(() => {
        pollExecution(runId).catch(() => null);
      }, 2000) as unknown as number;
    },
    [pollExecution, stopPollingExecution]
  );

  const handleRunPipeline = React.useCallback(async () => {
    if (!pipelineId || isGraphSavePending) {
      return;
    }

    try {
      setIsRunStarting(true);
      setExecution(null);
      notifiedTerminalStatusRef.current = null;
      const run = await runPipeline(pipelineId);
      setActiveRunId(run.run_id);
      localStorage.setItem(`pipeline_active_run_${pipelineId}`, run.run_id);
      toast.success('Запуск пайплайна начат');
      startPollingExecution(run.run_id);
    } catch (error) {
      toast.error(error instanceof Error ? error.message : 'Не удалось запустить пайплайн');
    } finally {
      setIsRunStarting(false);
    }
  }, [isGraphSavePending, pipelineId, startPollingExecution]);

  const handleDownloadResult = React.useCallback(() => {
    if (finalOutput === undefined) {
      toast.error('Финальный результат пока недоступен');
      return;
    }

    try {
      const blob = new Blob([JSON.stringify(finalOutput, null, 2)], {
        type: 'application/json',
      });
      const url = window.URL.createObjectURL(blob);
      const anchor = document.createElement('a');
      anchor.href = url;
      anchor.download = `pipeline-run-${activeRunId || 'result'}.json`;
      document.body.appendChild(anchor);
      anchor.click();
      document.body.removeChild(anchor);
      window.URL.revokeObjectURL(url);
    } catch {
      toast.error('Не удалось скачать результат');
    }
  }, [activeRunId, finalOutput]);

  React.useEffect(() => {
    if (!currentPipeline || !pipelineId) {
      setCanvasNodes([]);
      setCanvasEdges([]);
      setExpandedStep(null);
      setHasUnsavedGraphChanges(false);
      return;
    }

    const syncedNodes = syncNodesWithEdges(currentPipeline.nodes, currentPipeline.edges);
    const autoLayout = buildAutoLayout(syncedNodes, currentPipeline.edges);
    const storedLayout = parseLayout(localStorage.getItem(buildLayoutStorageKey(pipelineId)));

    const nextCanvasNodes = syncedNodes.map((node) => {
      const stepRunStatus = stepRunsByStep.get(node.step)?.status || null;
      const point = storedLayout[node.step] || autoLayout[node.step] || { x: 0, y: 0 };
      return {
        id: String(node.step),
        type: 'pipelineStep',
        position: point,
        data: {
          node,
          stepStatus: stepRunStatus,
        },
      } as Node<PipelineCanvasNodeData>;
    });

    const nextCanvasEdges = currentPipeline.edges.map((edge, index) =>
      toCanvasEdge(edge, index)
    );

    setCanvasNodes(nextCanvasNodes);
    setCanvasEdges(nextCanvasEdges);
    setExpandedStep(null);
    setHasUnsavedGraphChanges(false);
    setIsGraphSaveInFlight(false);
    setGraphRevision(0);
    lastSavedGraphRef.current = {
      pipelineId,
      nodes: syncedNodes,
      edges: currentPipeline.edges,
    };
  }, [currentPipeline, pipelineId, stepRunsByStep]);

  React.useEffect(() => {
    setCanvasNodes((prevNodes) =>
      prevNodes.map((node) => {
        const step = Number(node.id);
        const status = stepRunsByStep.get(step)?.status || null;
        if (node.data.stepStatus === status) {
          return node;
        }
        return {
          ...node,
          data: {
            ...node.data,
            stepStatus: status,
          },
        };
      })
    );
  }, [stepRunsByStep]);

  React.useEffect(() => {
    if (!pipelineId || canvasNodes.length === 0) {
      return;
    }

    const payload = canvasNodes.reduce<Record<string, { x: number; y: number }>>(
      (acc, node) => {
        acc[node.id] = {
          x: node.position.x,
          y: node.position.y,
        };
        return acc;
      },
      {}
    );
    localStorage.setItem(buildLayoutStorageKey(pipelineId), JSON.stringify(payload));
  }, [canvasNodes, pipelineId]);

  React.useEffect(() => {
    if (!pipelineId || !hasUnsavedGraphChanges || isGraphSaveInFlight) {
      return;
    }

    const debounceTimer = window.setTimeout(async () => {
      const revisionAtRequest = graphRevisionRef.current;
      const payloadNodes = canvasNodes.map((node) => node.data.node);
      const payloadEdges = toPipelineEdges(canvasEdges);
      const rollbackSnapshot = lastSavedGraphRef.current;

      setIsGraphSaveInFlight(true);
      try {
        const response = await updatePipelineGraph(pipelineId, {
          nodes: payloadNodes,
          edges: payloadEdges,
        });

        const normalizedNodes = syncNodesWithEdges(response.nodes, response.edges);

        if (graphRevisionRef.current !== revisionAtRequest) {
          lastSavedGraphRef.current = {
            pipelineId,
            nodes: normalizedNodes,
            edges: response.edges,
          };
          return;
        }

        const byStep = new Map<number, PipelineNode>();
        normalizedNodes.forEach((node) => byStep.set(node.step, node));

        setCanvasNodes((prevNodes) =>
          prevNodes.map((canvasNode) => {
            const step = Number(canvasNode.id);
            const updated = byStep.get(step);
            if (!updated) {
              return canvasNode;
            }
            return {
              ...canvasNode,
              data: {
                ...canvasNode.data,
                node: updated,
              },
            };
          })
        );
        setCanvasEdges(response.edges.map((edge, index) => toCanvasEdge(edge, index)));
        setHasUnsavedGraphChanges(false);

        lastSavedGraphRef.current = {
          pipelineId,
          nodes: normalizedNodes,
          edges: response.edges,
        };

        const pipelineSnapshot = latestPipelineRef.current;
        if (pipelineSnapshot && pipelineSnapshot.pipeline_id === pipelineId) {
          setPipeline({
            ...pipelineSnapshot,
            nodes: normalizedNodes,
            edges: response.edges,
          });
        }
      } catch (error) {
        if (
          rollbackSnapshot &&
          rollbackSnapshot.pipelineId === pipelineId &&
          graphRevisionRef.current === revisionAtRequest
        ) {
          const rollbackByStep = new Map<number, PipelineNode>();
          rollbackSnapshot.nodes.forEach((node) => rollbackByStep.set(node.step, node));

          setCanvasNodes((prevNodes) =>
            prevNodes.map((canvasNode) => {
              const step = Number(canvasNode.id);
              const rollbackNode = rollbackByStep.get(step);
              if (!rollbackNode) {
                return canvasNode;
              }
              return {
                ...canvasNode,
                data: {
                  ...canvasNode.data,
                  node: rollbackNode,
                },
              };
            })
          );
          setCanvasEdges(
            rollbackSnapshot.edges.map((edge, index) => toCanvasEdge(edge, index))
          );
          setHasUnsavedGraphChanges(false);
        }

        toast.error(
          error instanceof Error
            ? `Не удалось сохранить граф: ${error.message}`
            : 'Не удалось сохранить граф'
        );
      } finally {
        setIsGraphSaveInFlight(false);
      }
    }, 400);

    return () => {
      window.clearTimeout(debounceTimer);
    };
  }, [
    canvasEdges,
    canvasNodes,
    hasUnsavedGraphChanges,
    isGraphSaveInFlight,
    pipelineId,
    setPipeline,
  ]);

  React.useEffect(() => {
    if (pipelineId) {
      if (pipelineId !== lastPipelineIdRef.current) {
        setExecution(null);
        setActiveRunId(null);
        notifiedTerminalStatusRef.current = null;
        stopPollingExecution();
        lastPipelineIdRef.current = pipelineId;

        const savedRunId = localStorage.getItem(`pipeline_active_run_${pipelineId}`);
        if (savedRunId) {
          setActiveRunId(savedRunId);
          startPollingExecution(savedRunId);
        }
      }
    } else if (!isHydrating) {
      setExecution(null);
      setActiveRunId(null);
      notifiedTerminalStatusRef.current = null;
      stopPollingExecution();
      lastPipelineIdRef.current = null;
    }
  }, [pipelineId, isHydrating, stopPollingExecution, startPollingExecution]);

  React.useEffect(() => {
    return () => {
      stopPollingExecution();
    };
  }, [stopPollingExecution]);

  const onNodesChange = React.useCallback((changes: NodeChange[]) => {
    setCanvasNodes((prevNodes) => applyNodeChanges(changes, prevNodes));
  }, []);

  const isValidConnection = React.useCallback(
    (connection: Connection) => {
      const sourceStep = Number(connection.source);
      const targetStep = Number(connection.target);
      if (!Number.isInteger(sourceStep) || !Number.isInteger(targetStep)) {
        return false;
      }
      return !wouldCreateCycle(toPipelineEdges(canvasEdges), sourceStep, targetStep);
    },
    [canvasEdges]
  );

  const onConnect = React.useCallback(
    (connection: Connection) => {
      const sourceStep = Number(connection.source);
      const targetStep = Number(connection.target);
      if (!Number.isInteger(sourceStep) || !Number.isInteger(targetStep)) {
        return;
      }
      if (!isValidConnection(connection)) {
        toast.error('Эта связь создаёт цикл и запрещена');
        return;
      }
      openCreateEdgeDialog(sourceStep, targetStep);
    },
    [isValidConnection, openCreateEdgeDialog]
  );

  const onEdgesDelete = React.useCallback(
    (deleted: Array<Edge<PipelineCanvasEdgeData>>) => {
      if (!deleted.length) {
        return;
      }
      const deletedIds = new Set(deleted.map((edge) => edge.id));
      const nextEdges = canvasEdges.filter((edge) => !deletedIds.has(edge.id));
      setCanvasEdges(nextEdges);
      syncCanvasNodesConnections(nextEdges);
      markGraphAsDirty();
    },
    [canvasEdges, markGraphAsDirty, syncCanvasNodesConnections]
  );

  const onNewNodeDragStart = React.useCallback(
    (event: React.DragEvent<HTMLButtonElement>) => {
      event.dataTransfer.setData(NEW_NODE_DRAG_TYPE, 'pipelineStep');
      event.dataTransfer.effectAllowed = 'move';
    },
    []
  );

  const onCanvasDragOver = React.useCallback((event: React.DragEvent) => {
    event.preventDefault();
    event.dataTransfer.dropEffect = 'move';
  }, []);

  const onCanvasDrop = React.useCallback(
    (event: React.DragEvent) => {
      event.preventDefault();
      const droppedType = event.dataTransfer.getData(NEW_NODE_DRAG_TYPE);
      if (droppedType !== 'pipelineStep') {
        return;
      }
      if (!pipelineId) {
        toast.error('Сначала соберите или откройте пайплайн');
        return;
      }
      if (!reactFlowInstance || !reactFlowWrapperRef.current) {
        return;
      }

      const dropPosition = resolveDropPosition(
        reactFlowInstance,
        event,
        reactFlowWrapperRef.current.getBoundingClientRect()
      );

      let createdStep: number | null = null;
      setCanvasNodes((prevNodes) => {
        const nextStep = getNextStepNumber(prevNodes);
        createdStep = nextStep;
        return [
          ...prevNodes,
          {
            id: String(nextStep),
            type: 'pipelineStep',
            position: dropPosition,
            data: {
              node: createDraftPipelineNode(nextStep),
              stepStatus: null,
            },
          },
        ];
      });

      if (createdStep !== null) {
        setExpandedStep(createdStep);
      }
      markGraphAsDirty();
      toast.success('Новая нода добавлена');
    },
    [markGraphAsDirty, pipelineId, reactFlowInstance]
  );

  return (
    <>
      <Dialog open={edgeDialog.open} onOpenChange={(open) => !open && closeEdgeDialog()}>
        <DialogContent className="max-w-md">
          <DialogHeader>
            <DialogTitle>
              {edgeDialog.mode === 'create'
                ? 'Создать связь'
                : 'Изменить тип связи'}
            </DialogTitle>
            <DialogDescription>
              Step {edgeDialog.sourceStep} {'->'} Step {edgeDialog.targetStep}
            </DialogDescription>
          </DialogHeader>

          <div className="space-y-3">
            <Input
              value={edgeDialog.value}
              onChange={(event) =>
                setEdgeDialog((prev) => ({ ...prev, value: event.target.value }))
              }
              placeholder="Введите edge.type"
              autoFocus
            />
            {edgeDialog.suggestions.length > 0 && (
              <div className="space-y-1">
                <p className="text-xs text-muted-foreground">Подсказки:</p>
                <div className="flex flex-wrap gap-1.5">
                  {edgeDialog.suggestions.map((hint) => (
                    <Button
                      key={hint}
                      variant="outline"
                      size="sm"
                      className="h-7 text-[11px]"
                      onClick={() =>
                        setEdgeDialog((prev) => ({
                          ...prev,
                          value: hint,
                        }))
                      }
                    >
                      {hint}
                    </Button>
                  ))}
                </div>
              </div>
            )}
          </div>

          <DialogFooter className="flex items-center justify-between gap-2 sm:justify-between sm:space-x-0">
            {edgeDialog.mode === 'edit' && edgeDialog.edgeId ? (
              <Button
                variant="destructive"
                className="gap-2"
                onClick={() => deleteEdgeById(edgeDialog.edgeId!)}
              >
                <Trash2 className="h-4 w-4" /> Удалить
              </Button>
            ) : (
              <div />
            )}
            <div className="flex items-center gap-2">
              <Button variant="outline" onClick={closeEdgeDialog}>
                Отмена
              </Button>
              <Button onClick={handleConfirmEdgeDialog}>Сохранить</Button>
            </div>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <div className="h-full flex overflow-hidden">
        <div className="flex-1 relative bg-muted/5 bg-grid-pattern p-8 overflow-auto">
          <div className="space-y-8 py-10 min-w-0">
            <div className="flex flex-wrap items-center justify-between gap-4">
              <div>
                <h1 className="text-2xl font-bold text-foreground">Editor Pipeline</h1>
                <p className="mt-1 text-sm text-muted-foreground">
                  Соединяйте блоки через порты и редактируйте типы связей
                </p>
              </div>
              <div className="flex items-center gap-2">
                <Button
                  variant="outline"
                  className="gap-2 border-emerald-500/30 text-emerald-700 hover:bg-emerald-500/10"
                  draggable
                  onDragStart={onNewNodeDragStart}
                >
                  <Plus className="h-4 w-4" /> Новая нода (drag)
                </Button>
                <Badge
                  variant="outline"
                  className={cn(
                    'text-xs',
                    isGraphSaveInFlight
                      ? 'border-blue-500/40 bg-blue-500/10 text-blue-700'
                      : hasUnsavedGraphChanges
                        ? 'border-amber-500/40 bg-amber-500/10 text-amber-700'
                        : 'border-emerald-500/40 bg-emerald-500/10 text-emerald-700'
                  )}
                >
                  {isGraphSaveInFlight
                    ? 'Saving...'
                    : hasUnsavedGraphChanges
                      ? 'Unsaved changes'
                      : 'Saved'}
                </Badge>
              </div>
            </div>

            {isHydrating ? (
              <div className="flex flex-col items-center justify-center py-20 space-y-4">
                <Loader2 className="h-12 w-12 animate-spin text-primary/40" />
                <p className="text-sm text-muted-foreground animate-pulse">
                  Восстановление данных сессии...
                </p>
              </div>
            ) : currentPipeline ? (
              <Card
                ref={reactFlowWrapperRef}
                className="relative h-[560px] overflow-hidden border-primary/20 bg-card/60"
              >
                <ReactFlow
                  nodes={canvasNodes}
                  edges={canvasEdges}
                  nodeTypes={nodeTypes}
                  edgeTypes={edgeTypes}
                  onNodesChange={onNodesChange}
                  onConnect={onConnect}
                  onInit={setReactFlowInstance}
                  onDrop={onCanvasDrop}
                  onDragOver={onCanvasDragOver}
                  onNodeClick={(_, node) => setExpandedStep(Number(node.id))}
                  onEdgeClick={(_, edge) => openEditEdgeDialog(edge)}
                  onEdgesDelete={onEdgesDelete}
                  isValidConnection={isValidConnection}
                  fitView
                  deleteKeyCode={['Delete', 'Backspace']}
                  nodesDraggable
                  nodesConnectable
                  nodesFocusable
                  edgesFocusable
                  nodesDeletable={false}
                  defaultEdgeOptions={{
                    type: 'pipelineFlowEdge',
                    markerEnd: { type: MarkerType.ArrowClosed, color: '#22c55e' },
                  }}
                  minZoom={0.3}
                  maxZoom={1.8}
                  proOptions={{ hideAttribution: true }}
                >
                  <Background gap={24} size={1} />
                  <Controls showInteractive={false} />
                </ReactFlow>
                {canvasNodes.length === 0 ? (
                  <div className="pointer-events-none absolute inset-0 flex items-center justify-center">
                    <p className="rounded-full border border-emerald-500/25 bg-background/85 px-4 py-2 text-xs font-medium text-emerald-700">
                      Перетащите "Новая нода" на канвас
                    </p>
                  </div>
                ) : null}
              </Card>
            ) : (
              <Card className="mx-auto flex max-w-2xl items-center gap-4 border-dashed border-primary/20 bg-card p-8">
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

            {selectedNode && (
              <Card className="p-4 border-primary/10 bg-card/70">
                <div className="flex flex-wrap items-start justify-between gap-3">
                  <div>
                    <p className="text-[10px] font-bold uppercase tracking-[0.12em] text-primary/70">
                      Step {selectedNode.step}
                    </p>
                    <h3 className="text-base font-semibold text-foreground">
                      {selectedNode.name}
                    </h3>
                  </div>
                  <Button
                    variant="ghost"
                    size="icon"
                    className="h-8 w-8"
                    onClick={() => setExpandedStep(null)}
                  >
                    <X className="h-4 w-4" />
                  </Button>
                </div>
                <p className="mt-2 text-sm text-muted-foreground">
                  {selectedNode.description || 'Описание шага не указано.'}
                </p>

                <div className="mt-4 grid gap-3 text-xs text-muted-foreground sm:grid-cols-2">
                  <div>
                    <p className="font-semibold text-foreground">Inputs</p>
                    <p>
                      {selectedNode.input_connected_from.length > 0
                        ? `Step ${selectedNode.input_connected_from.join(', ')}`
                        : 'External'}
                    </p>
                  </div>
                  <div>
                    <p className="font-semibold text-foreground">Outputs</p>
                    <p>
                      {selectedNode.output_connected_to.length > 0
                        ? `Step ${selectedNode.output_connected_to.join(', ')}`
                        : 'Terminal'}
                    </p>
                  </div>
                </div>

                {selectedStepRun && (
                  <div className="mt-4 space-y-3">
                    {hasRequestBody(selectedStepRun.method) && (
                      <div>
                        <p className="text-xs font-semibold text-foreground">Принял</p>
                        <div className="mt-1">
                          <PayloadViewer payload={selectedStepRun.accepted_payload} />
                        </div>
                      </div>
                    )}
                    <div>
                      <p className="text-xs font-semibold text-foreground">Вернул</p>
                      <div className="mt-1">
                        <PayloadViewer payload={selectedStepRun.output_payload} />
                      </div>
                    </div>
                  </div>
                )}
              </Card>
            )}

            <Card className="mt-8 p-6 bg-primary/10 border-dashed border-primary/20 space-y-6">
              <div className="flex items-center justify-between gap-4">
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
                <Badge variant="outline" className={cn('text-xs', runStatusMeta.badgeClass)}>
                  {runStatusMeta.label}
                </Badge>
              </div>

              <div className="flex flex-wrap items-center justify-between gap-3">
                <div className="text-xs text-muted-foreground">
                  {activeRunId ? `Run ID: ${activeRunId}` : 'Запусков в текущей сессии пока нет'}
                </div>
                <div className="flex items-center gap-2">
                  <Button
                    variant="outline"
                    className="gap-2"
                    disabled={finalOutput === undefined}
                    onClick={handleDownloadResult}
                  >
                    <Download className="h-4 w-4" /> Скачать JSON
                  </Button>
                  <Button
                    className="gap-2"
                    disabled={!pipelineId || isRunStarting || isExecutionInProgress || isGraphSavePending}
                    onClick={handleRunPipeline}
                  >
                    {isRunStarting ? (
                      <>
                        <Loader2 className="h-4 w-4 animate-spin" /> Запуск...
                      </>
                    ) : (
                      <>
                        <Play className="h-4 w-4" /> Подтвердить и запустить
                      </>
                    )}
                  </Button>
                </div>
              </div>

              {isGraphSavePending && (
                <p className="text-xs text-amber-700">
                  Run временно заблокирован: дождитесь сохранения изменений графа.
                </p>
              )}

              {execution && (
                <div className="space-y-3 rounded-lg border border-border bg-background/70 p-4">
                  <div className="flex flex-wrap items-center gap-2">
                    <Badge variant="outline" className={cn('text-[11px]', runStatusMeta.badgeClass)}>
                      {execution.status}
                    </Badge>
                    {execution.error && (
                      <span className="text-xs text-red-700 break-words">{execution.error}</span>
                    )}
                  </div>
                  {execution.summary && (
                    <div className="grid grid-cols-2 gap-2 text-xs text-muted-foreground md:grid-cols-4">
                      <div>
                        <p className="font-semibold text-foreground">Total</p>
                        <p>{String(execution.summary.total_steps ?? '-')}</p>
                      </div>
                      <div>
                        <p className="font-semibold text-foreground">Succeeded</p>
                        <p>{String(execution.summary.succeeded_steps ?? '-')}</p>
                      </div>
                      <div>
                        <p className="font-semibold text-foreground">Failed</p>
                        <p>{String(execution.summary.failed_steps ?? '-')}</p>
                      </div>
                      <div>
                        <p className="font-semibold text-foreground">Skipped</p>
                        <p>{String(execution.summary.skipped_steps ?? '-')}</p>
                      </div>
                    </div>
                  )}
                  {execution.steps.length > 0 && (
                    <div className="space-y-2">
                      {execution.steps
                        .slice()
                        .sort((left, right) => left.step - right.step)
                        .map((stepRun) => {
                          const meta = getStepStatusMeta(stepRun.status);
                          return (
                            <div
                              key={`${stepRun.step}-${stepRun.created_at}`}
                              className="flex items-center justify-between rounded-md border border-border bg-card px-3 py-2 text-xs"
                            >
                              <div className="min-w-0">
                                <p className="font-semibold text-foreground truncate">
                                  Шаг {stepRun.step}: {stepRun.name || 'Без названия'}
                                </p>
                                {stepRun.error && (
                                  <p className="mt-0.5 text-red-700 truncate">{stepRun.error}</p>
                                )}
                              </div>
                              <Badge variant="outline" className={cn('ml-2', meta.badgeClass)}>
                                {meta.label}
                              </Badge>
                            </div>
                          );
                        })}
                    </div>
                  )}
                </div>
              )}
            </Card>
          </div>
        </div>

        <div
          className={cn(
            'h-full flex-shrink-0 transition-all duration-500 ease-in-out border-l border-border bg-card relative',
            isChatVisible ? 'w-80 opacity-100' : 'w-0 opacity-0 overflow-hidden border-none'
          )}
        >
          <div className="w-80 h-full absolute right-0 top-0">
            <SynthesisChat
              key={dialogId || 'active-dialog'}
              className="w-full h-full"
              initialMessage={initialMessage}
              initialDialogId={dialogId}
              onClose={() => setIsChatVisible(false)}
            />
          </div>
        </div>

        <AnimatePresence>
          {!isChatVisible && (
            <motion.div
              key="toggle"
              initial={{ opacity: 0, scale: 0.8, y: 20 }}
              animate={{ opacity: 1, scale: 1, y: 0 }}
              exit={{ opacity: 0, scale: 0.8, y: 20 }}
              className="fixed bottom-6 right-6 z-50"
            >
              <Button
                size="icon"
                className="h-14 w-14 rounded-full shadow-2xl bg-primary hover:bg-primary/90 text-primary-foreground group"
                onClick={() => setIsChatVisible(true)}
              >
                <MessageSquare className="h-6 w-6 transition-transform group-hover:scale-110" />
              </Button>
            </motion.div>
          )}
        </AnimatePresence>
      </div>
    </>
  );
};

export default Pipelines;
