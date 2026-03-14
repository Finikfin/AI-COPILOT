import React from 'react';
import { 
  Zap, 
  Plus, 
  Search, 
  MessageSquare, 
  Link2, 
  MoreVertical,
  BrainCircuit,
  Settings2
} from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import {
  Card,
  CardContent,
  CardDescription,
  CardFooter,
  CardHeader,
  CardTitle,
} from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';

// Mock data for Capabilities
const MOCK_BIO_CAPABILITIES = [
  { 
    id: 'c1', 
    name: 'Update Customer Profile', 
    description: 'Комплексная операция обновления данных пользователя: валидация полей, запись в БД и синхронизация с CRM.',
    actionsCount: 3,
    aiTag: 'Data Management',
    status: 'Ready'
  },
  { 
    id: 'c2', 
    name: 'Process Refund Ticket', 
    description: 'Инициирует возврат средств: проверяет статус транзакции в Stripe и создает тикет в Zendesk.',
    actionsCount: 2,
    aiTag: 'Finance',
    status: 'Alpha'
  },
  { 
    id: 'c3', 
    name: 'Sync Marketing Leads', 
    description: 'Автоматический сбор лидов из рекламных каналов и их распределение по менеджерам в Salesforce.',
    actionsCount: 4,
    aiTag: 'Marketing',
    status: 'Ready'
  },
  { 
    id: 'c4', 
    name: 'User Onboarding Flow', 
    description: 'Серия технических шагов для активации нового пользователя: создание аккаунта, отправка welcome-письма.',
    actionsCount: 5,
    aiTag: 'Operations',
    status: 'Stable'
  },
  { 
    id: 'c5', 
    name: 'Inventory Restock Alert', 
    description: 'Мониторинг остатков на складе и отправка алертов в Slack при достижении критического порога.',
    actionsCount: 2,
    aiTag: 'Logistics',
    status: 'Beta'
  }
];

const Capabilities: React.FC = () => {
  return (
    <div className="flex h-full flex-col px-4 sm:px-6 py-6 sm:py-8">
      {/* Header Section */}
      <div className="flex flex-col lg:flex-row lg:items-center justify-between mb-8 gap-6">
        <div>
          <h1 className="text-2xl font-semibold text-foreground flex items-center gap-2">
            <Zap className="h-6 w-6 text-primary fill-primary/20" />
            Capabilities Library
          </h1>
          <p className="text-muted-foreground mt-1 text-sm">
            Бизнес-навыки, созданные путем объединения нескольких API Actions. Обучены для понимания вашим ИИ.
          </p>
        </div>
        {/* Actions removed as requested */}
      </div>

      {/* Search/Filters */}
      <div className="mb-8">
        <div className="relative w-full sm:max-w-sm">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
          <Input 
            placeholder="Поиск по названию или тегу..." 
            className="pl-10 w-full bg-card border-border focus-visible:ring-primary"
          />
        </div>
      </div>

      {/* Grid Section - Added flex-1 and min-h-0 for proper scrolling behavior */}
      <div className="flex-1 min-h-0 overflow-auto">
        <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 2xl:grid-cols-4 gap-4 pb-10">
        {MOCK_BIO_CAPABILITIES.map((cap) => (
          <Card key={cap.id} className="bg-card border-border hover:border-primary/50 transition-all group overflow-hidden flex flex-col h-full min-h-[280px]">
            <CardHeader className="p-4 pb-2">
              <div className="flex items-start justify-between">
                <div className="bg-primary/10 p-1.5 rounded-lg mb-2 shrink-0">
                  <Zap className="h-4 w-4 text-primary" />
                </div>
                <Badge variant="outline" className="text-[9px] px-1.5 py-0 uppercase font-bold border-border bg-muted/50">
                  {cap.status}
                </Badge>
              </div>
              <CardTitle className="text-base text-foreground group-hover:text-primary transition-colors">
                {cap.name}
              </CardTitle>
              <Badge className="w-fit mt-1 text-[10px] px-2 py-0 bg-secondary text-secondary-foreground hover:bg-secondary border-border font-normal">
                {cap.aiTag}
              </Badge>
            </CardHeader>
            <CardContent className="p-4 pt-0 flex-1">
              <p className="text-[13px] text-muted-foreground leading-relaxed line-clamp-3">
                {cap.description}
              </p>
              
              <div className="mt-4 flex flex-col gap-2">
                <div className="flex items-center gap-2 text-xs text-muted-foreground">
                  <Link2 className="h-3 w-3" />
                  <span>Содержит {cap.actionsCount} Actions</span>
                </div>
                <div className="flex items-center gap-2 text-xs text-muted-foreground">
                  <MessageSquare className="h-3 w-3" />
                  <span>AI Semantic: Generated</span>
                </div>
              </div>
            </CardContent>
            <CardFooter className="p-3 border-t border-border bg-muted/20">
              <div className="flex items-center justify-between w-full">
                <Button variant="ghost" size="sm" className="h-7 px-2 text-[11px] text-foreground hover:text-primary">
                  Edit Mapping
                </Button>
                <Button variant="ghost" size="sm" className="h-7 w-7 p-0">
                  <Settings2 className="h-3.5 w-3.5 text-muted-foreground" />
                </Button>
              </div>
            </CardFooter>
          </Card>
        ))}

        {/* Create Placeholder Card */}
        <button className="border-2 border-dashed border-border rounded-xl flex flex-col items-center justify-center p-4 hover:border-primary/30 hover:bg-primary/5 transition-all text-muted-foreground hover:text-primary group min-h-[280px] h-full">
          <div className="w-10 h-10 rounded-full bg-muted flex items-center justify-center mb-3 group-hover:bg-primary/10">
            <Plus className="h-5 w-5" />
          </div>
          <span className="text-sm font-medium">Build New Capability</span>
          <span className="text-[11px] mt-1">Combine multiple API methods</span>
        </button>
        </div>
      </div>
    </div>
  );
};

export default Capabilities;
