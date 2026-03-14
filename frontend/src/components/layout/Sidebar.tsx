import React from 'react';
import { NavLink } from 'react-router-dom';
import { cn } from '@/lib/utils';
import { 
  Terminal, 
  Zap, 
  Workflow,
  ChevronLeft,
  Home
} from 'lucide-react';
import { Button } from '@/components/ui/button';

interface SidebarProps {
  isOpen: boolean;
  onClose: () => void;
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
  }
];

export const Sidebar: React.FC<SidebarProps> = ({ isOpen, onClose }) => {
  return (
    <>
      {/* Mobile overlay */}
      {isOpen && (
        <div
          className="fixed inset-0 z-20 bg-black bg-opacity-50 lg:hidden"
          onClick={onClose}
        />
      )}

      {/* Sidebar */}
      <aside
        className={cn(
          "fixed top-0 left-0 z-30 h-full bg-background border-r border-border transition-transform duration-300 ease-in-out",
          "lg:static lg:translate-x-0",
          isOpen ? "translate-x-0 w-64" : "-translate-x-full lg:w-16"
        )}
      >
        <div className="flex flex-col h-full">
          {/* Header */}
          <div className="flex items-center justify-between p-4 border-b border-border">
            {isOpen && <span className="font-medium text-foreground">Навигация</span>}
            <Button
              variant="ghost"
              size="sm"
              onClick={onClose}
              className="lg:hidden"
            >
              <ChevronLeft className="h-4 w-4" />
            </Button>
          </div>

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
            </ul>
          </nav>
        </div>
      </aside>
    </>
  );
};
