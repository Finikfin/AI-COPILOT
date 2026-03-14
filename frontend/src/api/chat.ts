import { ENDPOINTS } from '@/constants/api';

export interface GeneratePipelineRequest {
  dialog_id: string;
  message: string;
  user_id: string | null;
  capability_ids: string[] | null;
}

export interface GeneratePipelineResponse {
  status: 'success' | 'cannot_build' | 'error';
  message_ru: string;
  pipeline_id: string | null;
  nodes: any[];
  edges: any[];
  missing_requirements: string[];
  context_summary: string;
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
