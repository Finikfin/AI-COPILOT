import { ENDPOINTS } from '@/constants/api';

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
  const response = await fetch(ENDPOINTS.PIPELINES.GENERATE, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(request),
  });

  if (!response.ok) {
    throw new Error('Failed to generate pipeline');
  }

  return response.json();
};
