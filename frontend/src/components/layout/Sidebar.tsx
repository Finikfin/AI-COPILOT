import React, { useState } from 'react';
import { NavLink } from 'react-router-dom';
import { cn } from '@/lib/utils';
import {
  Terminal,
  Zap,
  Workflow,
  ChevronLeft,
  Home,
  Clock
} from 'lucide-react';
import { Button } from '@/components/ui/button';
import { HistoryDrawer } from '@/components/shared/HistoryDrawer';

interface SidebarProps {
  isOpen: boolean;
  onClose: () => void;
  isHistoryOpen: boolean;
  onToggleHistory: () => void;
}

const navigationItems = [
  {
    name: 'Home',
    href: '/',
    icon: Home
  },
  {
    name: 'Actions',
    href: '/actions',
    icon: Terminal
  },
  {
    name: 'Capabilities',
    href: '/capabilities',
    icon: Zap
  },
  {
    name: 'Pipelines',
    href: '/pipelines',
    icon: Workflow
  }
];

export const Sidebar: React.FC<SidebarProps> = ({ isOpen, onClose, isHistoryOpen, onToggleHistory }) => {
  return (
    <div className="flex h-full">
      {/* Mobile overlay */}
      {isOpen && (
        <div
          className="fixed inset-0 z-20 bg-black bg-opacity-50 lg:hidden"
          onClick={onClose}
        />
      )}

      {/* Main Sidebar */}
      <aside
        className={cn(
          "fixed top-16 left-0 z-30 h-[calc(100vh-4rem)] bg-background border-r border-border transition-transform duration-300 ease-in-out",
          "lg:static lg:top-0 lg:h-full lg:translate-x-0",
          isOpen ? "translate-x-0 w-64" : "-translate-x-full lg:w-16"
        )}
      >
        <div className="flex flex-col h-full">
          {/* Navigation */}
          <nav className="flex-1 p-2">
            <ul className="space-y-1">
              {navigationItems.map((item) => (
                <li key={item.name}>
                  <NavLink
                    to={item.href}
                    end={item.href === '/'}
                    className={({ isActive }) =>
                      cn(
                        "flex items-center gap-3 px-3 py-2 rounded-lg text-sm transition-colors",
                        "hover:bg-accent hover:text-accent-foreground",
                        isActive
                          ? "bg-primary/20 text-primary font-medium"
                          : "text-muted-foreground"
                      )
                    }
                    onClick={() => window.innerWidth < 1024 && onClose()}
                  >
                    <item.icon className="h-5 w-5 flex-shrink-0" />
                    {isOpen && <span>{item.name}</span>}
                  </NavLink>
                </li>
              ))}
              
              {/* History button */}
              <li>
                <button
                  onClick={onToggleHistory}
                  className={cn(
                    "flex items-center gap-3 px-3 py-2 rounded-lg text-sm transition-colors w-full",
                    "hover:bg-accent hover:text-accent-foreground",
                    isHistoryOpen ? "bg-primary/10 text-primary font-medium" : "text-muted-foreground"
                  )}
                >
                  <Clock className="h-5 w-5 flex-shrink-0" />
                  {isOpen && <span>History</span>}
                </button>
              </li>
            </ul>
          </nav>
        </div>
      </aside>

      {/* History Drawer (Next to Sidebar) */}
      <HistoryDrawer 
        isOpen={isHistoryOpen} 
        onClose={onToggleHistory} 
      />
    </div>
  );
};
