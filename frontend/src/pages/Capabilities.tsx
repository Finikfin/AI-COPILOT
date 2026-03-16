import {
  Zap,
  Plus,
  Search,
  Link2,
  FolderIcon,
  ExternalLink
} from 'lucide-react';
import React from 'react';
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from "@/components/ui/accordion";
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover";
import { cn } from '@/lib/utils';
import { Input } from '@/components/ui/input';
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { useActionsContext } from '@/contexts/ActionContext';

const Capabilities: React.FC = () => {
  const { actions, filteredCapabilities, searchTerm, setSearchTerm } = useActionsContext();

  const getMethodColor = (method: string) => {
    switch (method?.toUpperCase()) {
      case 'GET': return 'bg-green-500/10 text-green-500 border-green-500/20';
      case 'POST': return 'bg-blue-500/10 text-blue-500 border-blue-500/20';
      case 'PUT': return 'bg-cyan-500/10 text-cyan-500 border-cyan-500/20';
      case 'PATCH': return 'bg-yellow-500/10 text-yellow-500 border-yellow-500/20';
      case 'DELETE': return 'bg-red-500/10 text-red-500 border-red-500/20';
      default: return 'bg-gray-500/10 text-gray-500 border-gray-500/20';
    }
  };

  const groupedCapabilities = React.useMemo(() => {
    return filteredCapabilities.reduce((acc, cap) => {
      const associatedActions = actions.filter(a => a.id === cap.action_id);
      const action = associatedActions[0];
      const filename = action?.source_filename || 'General Capabilities';
      if (!acc[filename]) acc[filename] = [];
      acc[filename].push({ capability: cap, associatedActions });
      return acc;
    }, {} as Record<string, Array<{ capability: typeof filteredCapabilities[0], associatedActions: typeof actions }>>);
  }, [actions, filteredCapabilities]);

  return (
    <div className="flex h-full flex-col px-4 sm:px-6 py-6 sm:py-8">
      {/* Header Section */}
      <div className="flex flex-col lg:flex-row lg:items-center justify-between mb-8 gap-6">
        <div>
          <h1 className="text-2xl font-semibold text-foreground flex items-center gap-2">
            <Zap className="h-6 w-6 text-primary" />
            Capabilities Library
          </h1>
          <p className="text-muted-foreground mt-1 text-sm">
            Бизнес-навыки, созданные путем объединения нескольких API Actions. Обучены для понимания вашим ИИ.
          </p>
        </div>
      </div>

      {/* Search/Filters */}
      <div className="mb-8 flex items-center justify-between gap-4">
        <div className="relative w-full sm:max-w-sm">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
          <Input
            placeholder="Поиск по названию..."
            className="pl-10 w-full bg-card border-border focus-visible:ring-primary"
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
          />
        </div>

        <button className="hidden sm:flex items-center gap-2 text-primary hover:text-primary/80 transition-colors text-sm font-medium">
          <div className="w-8 h-8 rounded-lg bg-primary/10 flex items-center justify-center">
            <Plus className="h-4 w-4" />
          </div>
          Build New Capability
        </button>
      </div>

      {/* Grid Section - Grouped by Folders */}
      <div className="flex-1 min-h-0 overflow-auto pr-2 custom-scrollbar">
        {Object.keys(groupedCapabilities).length > 0 ? (
          <Accordion type="multiple" defaultValue={Object.keys(groupedCapabilities)} className="space-y-6 pb-10">
            {Object.entries(groupedCapabilities).map(([filename, items]) => (
              <AccordionItem key={filename} value={filename} className="border-none">
                <AccordionTrigger className="hover:no-underline py-2 mb-4 group border-b border-border/50">
                  <div className="flex items-center gap-2">
                    <FolderIcon className="h-4 w-4 text-primary opacity-70 group-hover:scale-110 transition-transform" />
                    <span className="text-sm font-bold text-foreground tracking-tight">{filename}</span>
                    <Badge variant="secondary" className="ml-2 text-[10px] px-1.5 py-0 bg-primary/5 text-primary border-primary/10">
                      {items.length}
                    </Badge>
                  </div>
                </AccordionTrigger>
                <AccordionContent className="pt-2 pb-6">
                  <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 2xl:grid-cols-4 gap-4">
                    {items.map(({ capability: cap, associatedActions }) => (
                      <Card key={cap.id} className="bg-card border-border hover:border-primary/50 transition-all group overflow-hidden flex flex-col h-full min-h-[180px] shadow-sm hover:shadow-md">
                        <CardHeader className="p-4 pb-2">
                          <div className="flex items-start justify-between">
                            <div className="bg-primary/10 p-1.5 rounded-lg mb-2 shrink-0">
                              <Zap className="h-4 w-4 text-primary" />
                            </div>
                          </div>
                          <CardTitle className="text-sm font-semibold text-foreground group-hover:text-primary transition-colors line-clamp-1">
                            {cap.name}
                          </CardTitle>
                        </CardHeader>
                        <CardContent className="p-4 pt-0 flex-1 flex flex-col justify-between">
                          <p className="text-[12px] text-muted-foreground leading-relaxed line-clamp-3">
                            {cap.description}
                          </p>

                          <div className="mt-4 pt-3 border-t border-border/50 flex items-center justify-between text-[10px] text-muted-foreground">
                            <Popover>
                              <PopoverTrigger asChild>
                                <button className="flex items-center gap-1.5 font-medium hover:text-primary transition-colors cursor-pointer group/link">
                                  <Link2 className="h-3 w-3 group-hover/link:scale-110 transition-transform" />
                                  <span>{associatedActions.length} Actions</span>
                                </button>
                              </PopoverTrigger>
                              <PopoverContent className="w-80 p-0 overflow-hidden border-border bg-card shadow-2xl">
                                <div className="p-3 border-b border-border bg-muted/30">
                                  <h4 className="text-xs font-bold text-foreground">Связанные API Методы</h4>
                                </div>
                                <div className="max-h-[300px] overflow-auto custom-scrollbar">
                                  {associatedActions.length > 0 ? (
                                    <div className="divide-y divide-border/50">
                                      {associatedActions.map((action) => (
                                        <div key={action.id} className="p-3 hover:bg-muted/50 transition-colors">
                                          <div className="flex items-center gap-2 mb-1">
                                            <Badge variant="outline" className={cn("px-1 py-0 text-[9px] font-bold border-none h-4", getMethodColor(action.method))}>
                                              {action.method}
                                            </Badge>
                                            <code className="text-[10px] text-foreground font-mono truncate max-w-[180px]">
                                              {action.path}
                                            </code>
                                          </div>
                                          <p className="text-[10px] text-muted-foreground line-clamp-2 leading-relaxed">
                                            {action.summary || action.description || 'Нет описания'}
                                          </p>
                                        </div>
                                      ))}
                                    </div>
                                  ) : (
                                    <div className="p-8 text-center">
                                      <p className="text-xs text-muted-foreground">Методы не найдены</p>
                                    </div>
                                  )}
                                </div>
                                <div className="p-2 border-t border-border bg-muted/10">
                                  <button className="w-full text-[10px] text-primary hover:underline font-medium py-1">
                                    Manage Connections
                                  </button>
                                </div>
                              </PopoverContent>
                            </Popover>
                            <button className="text-primary hover:underline font-semibold">Detail</button>
                          </div>
                        </CardContent>
                      </Card>
                    ))}
                  </div>
                </AccordionContent>
              </AccordionItem>
            ))}
          </Accordion>
        ) : (
          <div className="flex flex-col items-center justify-center py-20 opacity-60">
            <Zap className="h-12 w-12 text-muted-foreground mb-4" />
            <p className="text-sm">No capabilities found</p>
          </div>
        )}
      </div>
    </div>
  );
};

export default Capabilities;
