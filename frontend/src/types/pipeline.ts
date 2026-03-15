export interface PipelineNode {
  step: number;
  name: string;
  description: string;
  input_connected_from: number[];
  output_connected_to: number[];
  input_data_type_from_previous: Array<{ from_step: number; type: string }>;
  external_inputs: string[];
  endpoints: Array<{
    name: string;
    capability_id: string;
    action_id: string;
    input_type: Record<string, string> | string | null;
    output_type: string | Record<string, string> | null;
  }>;
}

export interface PipelineEdge {
  from_step: number;
  to_step: number;
  type: string;
}

export interface PipelineData {
  status: 'ready' | 'success' | 'needs_input' | 'cannot_build';
  message_ru: string;
  chat_reply_ru: string;
  pipeline_id: string | null;
  nodes: PipelineNode[];
  edges: PipelineEdge[];
  missing_requirements: string[];
  context_summary: string | null;
}
