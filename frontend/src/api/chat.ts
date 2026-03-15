import { ENDPOINTS } from '@/constants/api';
import { apiRequest } from '@/lib/api';

export interface GeneratePipelineRequest {
  dialog_id: string;
  message: string;
  user_id: string | null;
  capability_ids: string[] | null;
}

export interface GeneratePipelineResponse {
  status: 'ready' | 'success' | 'needs_input' | 'cannot_build' | 'error';
  message_ru: string;
  chat_reply_ru?: string;
  pipeline_id: string | null;
  nodes: any[];
  edges: any[];
  missing_requirements?: string[];
  context_summary: string | null;
}

export const generatePipeline = async (request: GeneratePipelineRequest): Promise<GeneratePipelineResponse> => {
  return apiRequest<GeneratePipelineResponse>(ENDPOINTS.PIPELINES.GENERATE, {
    method: 'POST',
    body: JSON.stringify(request),
  });
};

export interface PipelineDialogListItem {
  dialog_id: string;
  title: string | null;
  last_status: string | null;
  last_pipeline_id: string | null;
  last_message_preview: string | null;
  created_at: string;
  updated_at: string;
}

export interface PipelineDialogHistoryMessage {
  id: string;
  role: 'user' | 'assistant';
  content: string;
  assistant_payload: GeneratePipelineResponse | null;
  created_at: string;
}

export interface PipelineDialogHistoryResponse {
  dialog_id: string;
  title: string | null;
  messages: PipelineDialogHistoryMessage[];
}

export const listPipelineDialogs = async (
  limit = 20,
  offset = 0
): Promise<PipelineDialogListItem[]> => {
  const query = new URLSearchParams({
    limit: String(limit),
    offset: String(offset),
  });
  return apiRequest<PipelineDialogListItem[]>(`${ENDPOINTS.PIPELINES.DIALOGS}?${query.toString()}`);
};

export const getPipelineDialogHistory = async (
  dialogId: string,
  limit = 30,
  offset = 0
): Promise<PipelineDialogHistoryResponse> => {
  const query = new URLSearchParams({
    limit: String(limit),
    offset: String(offset),
  });
  return apiRequest<PipelineDialogHistoryResponse>(`${ENDPOINTS.PIPELINES.DIALOG_HISTORY(dialogId)}?${query.toString()}`);
};
