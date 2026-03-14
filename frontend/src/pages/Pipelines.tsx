/**
 * @fileoverview Graph Editor page component for Krok MVP
 * 
 * This component provides an interactive graph visualization and editing interface
 * for infrastructure topology. Users can create, edit, and manage nodes and
 * connections between different system components.
 * 
 * Features:
 * - Interactive node creation and editing
 * - Drag-and-drop node placement
 * - Connection management between nodes
 * - Multiple flow support with tabs
 * - Zoom and pan controls
 * - Node properties panel
 * - Import/export functionality
 * - Real-time health monitoring
 * 
 * @author Krok Development Team
 * @version 1.0.0
 */

import React, { useState, useCallback } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { 
  Save, 
  Download, 
  Upload, 
  ZoomIn, 
  ZoomOut,
  RotateCcw,
  Trash2,
  Settings,
} from "lucide-react";
import { GraphNode as GraphNodeType, GraphLink } from "@/types/graph";
import { GraphNode } from "@/components/graph/GraphNode";
import { NodePalette } from "@/components/graph/NodePalette";
import { PropertiesPanel } from "@/components/graph/PropertiesPanel";
import { GraphLinkLine } from "@/components/graph/GraphLinkLine";
import { toast } from "sonner";
import { getPortCenter } from "@/lib/portUtils";
import { useMousePosition } from "@/lib/useMousePosition";
import {
  generateNodeId,
  generateLinkId,
  generateFlowId,
  ensureNodesHaveHealth,
  ensureNodeListHasHealth,
} from "@/lib/utils";
import ReactModal from "react-modal";
import { NodeDetailsModal } from "@/components/graph/NodeDetailsModal";
import { GraphEditorHeader } from "@/components/graph/GraphEditorHeader";
import { FlowTabs } from "@/components/graph/FlowTabs";
import { CanvasInfoOverlay } from "@/components/graph/CanvasInfoOverlay";
import { useGraphEditorState } from "@/lib/useGraphEditorState";

// Новый тип для Flow
interface Flow {
  id: string;
  name: string;
  nodes: GraphNodeType[];
  links: GraphLink[];
}

export const GraphEditor: React.FC = () => {
  const {
    flows,
    setFlows,
    activeFlowId,
    setActiveFlowId,
    selectedNodeId,
    setSelectedNodeId,
    selectedLinkId,
    setSelectedLinkId,
    zoom,
    setZoom,
    hasChanges,
    setHasChanges,
    dragPort,
    setDragPort,
    modalNodeId,
    setModalNodeId,
    handleAddFlow,
    handleSelectFlow,
    handleRenameFlow,
    handleDeleteFlow,
    setNodes,
    setLinks,
    handleAddNode,
    handleSelectNode,
    handleDragNode,
    handleUpdateNode,
    handleDeleteNode,
    handleSave,
    handleExport,
    handleImport,
    handleZoomIn,
    handleZoomOut,
    handleReset,
    handleClearAll,
    handlePortConnectStart,
    handlePortConnectEnd,
    handleCanvasDrop,
    handleCanvasDragOver,
    handleAddNodeAt,
  } = useGraphEditorState();

  const activeFlow = flows.find((f) => f.id === activeFlowId)!;
  const nodes = activeFlow.nodes.map((node) => ({
    ...node,
    health:
      typeof node.health === "number"
        ? node.health
        : Math.floor(Math.random() * 100),
  }));
  const links = activeFlow.links;
  const selectedNode = nodes.find((node) => node.id === selectedNodeId) || null;
  const mousePos = useMousePosition();
  const modalNode = nodes.find((n) => n.id === modalNodeId) || null;

  return (
    <div className="h-full flex flex-col">
      {/* Header */}
      <GraphEditorHeader
        hasChanges={hasChanges}
        nodesCount={nodes.length}
        zoom={zoom}
        onImport={handleImport}
        onExport={handleExport}
        onSave={handleSave}
      />

      {/* UI для flows (сверху) */}
      <FlowTabs
        flows={flows}
        activeFlowId={activeFlowId}
        onSelectFlow={handleSelectFlow}
        onAddFlow={handleAddFlow}
        onDeleteFlow={handleDeleteFlow}
        onRenameFlow={handleRenameFlow}
      />

      <div className="flex-1 flex">
        {/* Tool Palette */}
        <div className="w-64 p-4 bg-gray-50 border-r border-gray-200">
          <NodePalette onAddNode={handleAddNode} />
        </div>

        {/* Main Canvas + Properties Panel */}
        <div className="flex-1 relative bg-white overflow-hidden flex">
          {/* Canvas + overlay */}
          <div className="flex-1 relative">
            <div
              id="graph-canvas"
              className="absolute inset-0 bg-[#f7f7fa] bg-grid-pattern"
              style={{
                transform: `scale(${zoom})`,
                transformOrigin: "top left",
                backgroundSize: `${20 * zoom}px ${20 * zoom}px`,
                pointerEvents: "auto",
                zIndex: 10,
              }}
              onDrop={handleCanvasDrop}
              onDragOver={handleCanvasDragOver}
            >
              {/* Overlay только над canvas */}
              {modalNode && (
                <div className="absolute inset-0 bg-black bg-opacity-40 z-[90] transition-opacity duration-300 pointer-events-none" />
              )}
              {/* SVG слой для связей */}
              <svg
                className="absolute inset-0 w-full h-full"
                style={{ zIndex: 1, pointerEvents: "none" }} // SVG не перехватывает события
              >
                <defs>
                  <marker
                    id="arrowhead"
                    markerWidth="10"
                    markerHeight="7"
                    refX="10"
                    refY="3.5"
                    orient="auto"
                    markerUnits="strokeWidth"
                  >
                    <polygon points="0 0, 10 3.5, 0 7" fill="#22c55e" />
                  </marker>
                </defs>
                {links.map((link) => {
                  const source = nodes.find(
                    (n) => n.id === link.source.split(":")[0]
                  );
                  const target = nodes.find(
                    (n) => n.id === link.target.split(":")[0]
                  );
                  if (!source || !target) return null;
                  const sourceIdx = parseInt(
                    link.source.split(":")[1] || "0",
                    10
                  );
                  const targetIdx = parseInt(
                    link.target.split(":")[1] || "0",
                    10
                  );
                  // Получаем DOM-элементы узлов
                  const sourceNodeEl = document.querySelector(
                    `[data-node-id="${source.id}"]`
                  );
                  const targetNodeEl = document.querySelector(
                    `[data-node-id="${target.id}"]`
                  );
                  let sx, sy, tx, ty;
                  if (sourceNodeEl) {
                    const portCenter = getPortCenter(
                      sourceNodeEl as HTMLElement,
                      "output",
                      sourceIdx
                    );
                    const canvas = document.getElementById("graph-canvas");
                    const rect = canvas?.getBoundingClientRect();
                    if (portCenter && rect) {
                      sx = (portCenter.x - rect.left) / zoom;
                      sy = (portCenter.y - rect.top) / zoom;
                    }
                  }
                  if (targetNodeEl) {
                    const portCenter = getPortCenter(
                      targetNodeEl as HTMLElement,
                      "input",
                      targetIdx
                    );
                    const canvas = document.getElementById("graph-canvas");
                    const rect = canvas?.getBoundingClientRect();
                    if (portCenter && rect) {
                      tx = (portCenter.x - rect.left) / zoom;
                      ty = (portCenter.y - rect.top) / zoom;
                    }
                  }
                  // fallback если не удалось получить DOM
                  if (sx === undefined || sy === undefined) {
                    const sourceY =
                      source.y +
                      40 +
                      (source.output && Array.isArray(source.output)
                        ? (sourceIdx - (source.output.length - 1) / 2) * 16
                        : 0);
                    sx = source.x + 120 + 8;
                    sy = sourceY + 8;
                  }
                  if (tx === undefined || ty === undefined) {
                    const targetY =
                      target.y +
                      40 +
                      (target.input && Array.isArray(target.input)
                        ? (targetIdx - (target.input.length - 1) / 2) * 16
                        : 0);
                    tx = target.x + 8;
                    ty = targetY + 8;
                  }
                  return (
                    <GraphLinkLine
                      key={link.id}
                      source={{ x: sx, y: sy, width: 0, height: 0 }}
                      target={{ x: tx, y: ty, width: 0, height: 0 }}
                      color={
                        link.status === "active"
                          ? "#22c55e"
                          : link.status === "error"
                          ? "#ef4444"
                          : "#a3a3a3"
                      }
                      markerEnd={false}
                      opacity={0.8}
                    />
                  );
                })}
                {dragPort &&
                  mousePos &&
                  (() => {
                    const source = nodes.find((n) => n.id === dragPort.nodeId);
                    if (!source) return null;
                    const sourceIdx = dragPort.portIdx;
                    // Получаем DOM-элемент узла-источника
                    const sourceNodeEl = document.querySelector(
                      `[data-node-id="${source.id}"]`
                    );
                    let startX, startY;
                    if (sourceNodeEl) {
                      const portCenter = getPortCenter(
                        sourceNodeEl as HTMLElement,
                        "output",
                        sourceIdx
                      );
                      const canvas = document.getElementById("graph-canvas");
                      const rect = canvas?.getBoundingClientRect();
                      if (portCenter && rect) {
                        startX = (portCenter.x - rect.left) / zoom;
                        startY = (portCenter.y - rect.top) / zoom;
                      }
                    }
                    // Если не удалось получить координаты — fallback
                    if (startX === undefined || startY === undefined) {
                      const sourceY =
                        source.y +
                        40 +
                        (source.output && Array.isArray(source.output)
                          ? (sourceIdx - (source.output.length - 1) / 2) * 16
                          : 0);
                      startX = source.x + 120 + 8;
                      startY = sourceY + 8;
                    }
                    // Конец линии: если мышь над input-портом, берём его центр
                    let endX =
                      (mousePos.x -
                        (document
                          .getElementById("graph-canvas")
                          ?.getBoundingClientRect().left || 0)) /
                      zoom;
                    let endY =
                      (mousePos.y -
                        (document
                          .getElementById("graph-canvas")
                          ?.getBoundingClientRect().top || 0)) /
                      zoom;
                    const overInput = document.elementFromPoint(
                      mousePos.x,
                      mousePos.y
                    );
                    if (
                      overInput &&
                      overInput.getAttribute &&
                      overInput.getAttribute("data-port") === "input"
                    ) {
                      // Ищем родительский узел
                      let inputNodeEl = overInput.closest("[data-node-id]");
                      if (inputNodeEl) {
                        // Определяем индекс input-порта
                        const inputPorts = inputNodeEl.querySelectorAll(
                          '[data-port="input"]'
                        );
                        let inputIdx = Array.from(inputPorts).findIndex(
                          (el) => el === overInput
                        );
                        if (inputIdx !== -1) {
                          const portCenter = getPortCenter(
                            inputNodeEl as HTMLElement,
                            "input",
                            inputIdx
                          );
                          const canvas =
                            document.getElementById("graph-canvas");
                          const rect = canvas?.getBoundingClientRect();
                          if (portCenter && rect) {
                            endX = (portCenter.x - rect.left) / zoom;
                            endY = (portCenter.y - rect.top) / zoom;
                          }
                        }
                      }
                    }
                    // Bezier control points
                    const dx = Math.max(Math.abs(endX - startX) * 0.5, 40);
                    const c1x = startX + dx;
                    const c1y = startY;
                    const c2x = endX - dx;
                    const c2y = endY;
                    return (
                      <path
                        d={`M${startX},${startY} C${c1x},${c1y} ${c2x},${c2y} ${endX},${endY}`}
                        fill="none"
                        stroke="#22c55e"
                        strokeWidth={3}
                        opacity={0.7}
                      />
                    );
                  })()}
              </svg>
              {nodes.length === 0 ? (
                <div className="absolute inset-0 flex items-center justify-center pointer-events-none">
                  <div className="text-center text-gray-500">
                    <div className="w-16 h-16 mx-auto mb-4 bg-white rounded-lg border border-gray-300 flex items-center justify-center">
                      <Settings className="h-8 w-8" />
                    </div>
                    <h3 className="text-lg font-medium mb-2">
                      Начните создание графа
                    </h3>
                    <p className="text-sm">
                      Добавьте компоненты из панели инструментов
                      <br />
                      или импортируйте существующую топологию
                    </p>
                  </div>
                </div>
              ) : (
                <>
                  {nodes.map((node) => (
                    <GraphNode
                      key={node.id}
                      node={node}
                      isSelected={selectedNodeId === node.id}
                      onSelect={setSelectedNodeId}
                      onDrag={handleDragNode}
                      onDelete={handleDeleteNode}
                      onPortConnectStart={handlePortConnectStart}
                      onPortConnectEnd={handlePortConnectEnd}
                      dragPort={dragPort}
                      links={links}
                      nodes={nodes}
                      onDoubleClick={() => setModalNodeId(node.id)}
                    />
                  ))}
                </>
              )}
            </div>
            <CanvasInfoOverlay />
          </div>
          {/* Properties Panel (всегда видна) */}
          <div className="w-80 p-4 bg-gray-50 border-l border-gray-200 h-full relative z-[100]">
            {/* Информация о потоке: редактирование имени */}
            <div className="mb-4">
              <div className="text-xs text-gray-500 mb-1">Имя потока</div>
              <input
                className="w-full px-2 py-1 border rounded bg-white text-sm font-bold"
                value={activeFlow.name}
                onChange={(e) =>
                  handleRenameFlow(activeFlow.id, e.target.value)
                }
              />
            </div>
            <PropertiesPanel
              selectedNode={selectedNode}
              onUpdateNode={handleUpdateNode}
              onDeleteNode={handleDeleteNode}
              links={links}
              onSelectLink={setSelectedLinkId}
              selectedLinkId={selectedLinkId}
              flow={activeFlow}
              isNodeSelected={!!selectedNode}
            />
          </div>
          {/* NodeDetailsModal - рендерится только когда открыт */}
          <NodeDetailsModal
            node={
              modalNode ||
              nodes[0] || {
                id: "",
                type: "inject" as any,
                name: "",
                x: 0,
                y: 0,
                health: 0,
                status: "unknown" as any,
                properties: {},
              }
            }
            isOpen={!!modalNode}
            onClose={() => setModalNodeId(null)}
            onUpdate={handleUpdateNode}
          />
        </div>
      </div>
    </div>
  );
};

export default GraphEditor;
