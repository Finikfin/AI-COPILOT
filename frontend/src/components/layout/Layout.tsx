/**
 * @fileoverview Main layout component for Krok MVP
 * 
 * This component provides the primary layout structure for the application,
 * including the header, sidebar navigation, and main content area. It
 * manages the responsive sidebar state and provides a consistent layout
 * across all pages.
 * 
 * Features:
 * - Responsive header with navigation controls
 * - Collapsible sidebar navigation
 * - Main content area with routing
 * - Chat button for user support
 * - Mobile-responsive design
 * 
 * @author Krok Development Team
 * @version 1.0.0
 */

import React, { useState } from 'react';
import { Outlet } from 'react-router-dom';
import { Header } from './Header';
import { Sidebar } from './Sidebar';

/**
 * Layout component
 * 
 * Main layout wrapper that provides the application structure with header,
 * sidebar, and main content area. Manages sidebar state and provides
 * consistent navigation across all pages.
 * 
 * @returns JSX.Element - The complete application layout
 */
export const Layout: React.FC = () => {
  /**
   * State to control sidebar visibility
   * Used for mobile responsive design
   */
  const [sidebarOpen, setSidebarOpen] = useState(false);
  const [historyOpen, setHistoryOpen] = useState(false);

  /**
   * Toggles the sidebar open/closed state
   */
  const toggleSidebar = () => setSidebarOpen(!sidebarOpen);
  
  /**
   * Closes the sidebar
   */
  const closeSidebar = () => setSidebarOpen(false);

  return (
    <div className="h-screen flex flex-col bg-background">
      {/* Application header with navigation controls */}
      <Header onToggleSidebar={toggleSidebar} />
      
      {/* Main content area with sidebar and page content */}
      <div className="flex-1 flex overflow-hidden">
        {/* Collapsible sidebar navigation */}
        <Sidebar 
          isOpen={sidebarOpen} 
          onClose={closeSidebar} 
          isHistoryOpen={historyOpen}
          onToggleHistory={() => setHistoryOpen(!historyOpen)}
        />
        
        {/* Main content area with routing */}
        <main className="flex-1 overflow-auto relative">
          <div className="h-full">
            <Outlet />
          </div>
        </main>
      </div>
    </div>
  );
};