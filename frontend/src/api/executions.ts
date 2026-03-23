import { ENDPOINTS } from '@/constants/api';
import { apiRequest } from '@/lib/api';

export type ExecutionRunStatus =
  | 'QUEUED'
  | 'RUNNING'
  | 'SUCCEEDED'
  | 'FAILED'
  | 'PARTIAL_FAILED';

export type ExecutionStepStatus =
  | 'PENDING'
  | 'RUNNING'
  | 'SUCCEEDED'
  | 'FAILED'
  | 'SKIPPED';

export type ExecutionHttpMethod =
  | 'GET'
  | 'POST'
  | 'PUT'
  | 'PATCH'
  | 'DELETE'
  | 'HEAD'
  | 'OPTIONS';

export interface RunPipelineRequest {
  inputs?: Record<string, unknown>;
  dialog_id?: string | null;
}

export interface RunPipelineResponse {
  run_id: string;
  pipeline_id: string;
  status: 'QUEUED' | 'RUNNING';
}

export interface ExecutionStepRunResponse {
  step: number;
  name: string | null;
  capability_id: string | null;
  action_id: string | null;
  method: ExecutionHttpMethod | null;
  status_code: number | null;
  status: ExecutionStepStatus;
  resolved_inputs: Record<string, unknown> | null;
  accepted_payload: unknown;
  output_payload: unknown;
  request_snapshot: Record<string, unknown> | null;
  response_snapshot: Record<string, unknown> | null;
  error: string | null;
  started_at: string | null;
  finished_at: string | null;
  duration_ms: number | null;
  created_at: string;
  updated_at: string;
}

export interface ExecutionRunDetailResponse {
  id: string;
  pipeline_id: string;
  status: ExecutionRunStatus;
  inputs: Record<string, unknown>;
  summary: Record<string, unknown> | null;
  error: string | null;
  started_at: string | null;
  finished_at: string | null;
  created_at: string;
  updated_at: string;
  steps: ExecutionStepRunResponse[];
}

export const runPipeline = async (
  pipelineId: string,
  payload: RunPipelineRequest = {}
): Promise<RunPipelineResponse> => {
  return apiRequest<RunPipelineResponse>(ENDPOINTS.PIPELINES.RUN(pipelineId), {
    method: 'POST',
    body: JSON.stringify({
      inputs: payload.inputs ?? {},
      dialog_id: payload.dialog_id ?? null,
    }),
  });
};

export const getExecution = async (
  runId: string
): Promise<ExecutionRunDetailResponse> => {
  return apiRequest<ExecutionRunDetailResponse>(ENDPOINTS.EXECUTIONS.GET(runId));
};
