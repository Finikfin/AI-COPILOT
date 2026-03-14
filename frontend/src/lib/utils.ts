/**
 * @fileoverview Utility functions for AI Copilot application
 * 
 * This file contains common utility functions used throughout the application,
 * including CSS class merging, ID generation, and data transformation helpers.
 * These utilities provide reusable functionality for the graph editor and
 * other components.
 * 
 * Features:
 * - CSS class name merging with Tailwind CSS
 * - Unique ID generation for nodes, links, and flows
 * - Health data assignment for graph nodes
 * - Data transformation utilities
 * 
 * @author Krok Development Team
 * @version 1.0.0
 */

import { clsx, type ClassValue } from "clsx";
import { twMerge } from "tailwind-merge";

/**
 * Merges CSS class names with Tailwind CSS conflict resolution
 * 
 * Combines multiple class names and resolves conflicts using Tailwind's
 * merge functionality. This ensures proper CSS class precedence.
 * 
 * @param inputs - CSS class names to merge
 * @returns string - Merged CSS class string
 */
export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

/**
 * Generates a unique node ID
 * 
 * Creates a unique identifier for graph nodes using timestamp and
 * random string combination.
 * 
 * @returns string - Unique node ID
 */
export function generateNodeId() {
  return `node_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
}

/**
 * Generates a unique link ID
 * 
 * Creates a unique identifier for graph links using timestamp and
 * random string combination.
 * 
 * @returns string - Unique link ID
 */
export function generateLinkId() {
  return `link_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
}

/**
 * Generates a unique flow ID
 * 
 * Creates a unique identifier for graph flows using timestamp and
 * random string combination.
 * 
 * @returns string - Unique flow ID
 */
export function generateFlowId() {
  return `flow_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
}

/**
 * Ensures all nodes in flows have health data
 * 
 * Iterates through all flows and assigns random health values to nodes
 * that don't already have health data. Used for initializing graph data.
 * 
 * @param flows - Array of flow objects containing nodes
 * @returns Array - Flows with health data assigned to all nodes
 */
export function ensureNodesHaveHealth(flows) {
  return flows.map((flow) => ({
    ...flow,
    nodes: flow.nodes.map((node) => ({
      ...node,
      health:
        typeof node.health === "number"
          ? node.health
          : Math.floor(Math.random() * 100),
    })),
  }));
}

/**
 * Ensures all nodes in a list have health data
 * 
 * Assigns random health values to nodes that don't already have health data.
 * Used for initializing individual node lists.
 * 
 * @param nodes - Array of node objects
 * @returns Array - Nodes with health data assigned
 */
export function ensureNodeListHasHealth(nodes) {
  return nodes.map((node) => ({
    ...node,
    health:
      typeof node.health === "number"
        ? node.health
        : Math.floor(Math.random() * 100),
  }));
}
/**
 * Generates a unique UUID
 * 
 * Uses the Web Crypto API if available, otherwise falls back to a 
 * Math.random() based implementation for non-secure contexts.
 * 
 * @returns string - Unique UUID
 */
export function generateUUID() {
  if (typeof crypto !== 'undefined' && crypto.randomUUID) {
    return crypto.randomUUID();
  }
  // Fallback for non-secure contexts
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
    const r = (Math.random() * 16) | 0;
    const v = c === 'x' ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
}
