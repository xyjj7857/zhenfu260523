export interface BinanceSettings {
  apiKey: string;
  secretKey: string;
  baseUrl: string;
  wsUrl: string;
  positionModeChecked?: boolean; // 新增：是否已经检查并修复过仓位模式
}

export interface ScannerSettings {
  stage0: {
    interval: string;
    startTime: string;
    klinePeriod: string;
    minKlines: number;
    maxKlines: number;
    includeTradFi: boolean;
  };
  stage0P: {
    enabled: boolean;
    interval: string;
    startTime: string;
    periods: {
      [key: string]: {
        enabled: boolean;
        count: number;
        threshold: number;
      };
    };
    abnormalMove?: {
      enabled: boolean;
      lookbackHours: number;    // 考察时长 (如 10)
      windowMinutes: number;    // 统计区间 (如 60)
      maxPump: number;         // 最大涨幅 (%)
      maxDrop: number;         // 最大跌幅 (%)
    };
  };
  stage1: {
    interval: string;
    startTime: string;
    minVolumeM1: number;
    priceChangeK1: [number, number];
    whitelist: string[];
    blacklist: string[];
  };
  stage2: {
    interval: string;
    startTime: string;
    cooldown: number;
    preferredMode?: 'volume' | 'amp'; // 优选模式
    amplitudeMode?: 'bottomHigh' | 'highLow'; // 振幅计算方式：bottomHigh (底高模式) | highLow (高低模式)
    conditions: {
      amp: { enabled: boolean; range: [number, number] };
      longShort: { enabled: boolean; buyEnabled: boolean; buy: number; sellEnabled: boolean; sell: number };
      m: { enabled: boolean; range: [number, number] };
      fundingRateOptimization?: {
        enabled: boolean;
        windowMinutes: number;
        shortThreshold: number; // Percent, e.g., -0.3
      };
    };
  };
  timeControl: {
    enabled: boolean;
    hours: boolean[];
    mode: '+2' | '-2';
  };
}

export interface OrderSettings {
  leverage: number;
  positionRatio: number;
  maxPosition: number;
  tpModeBuy: 'ratio' | 'fixed' | 'amp';
  tpModeSell: 'ratio' | 'fixed' | 'amp';
  tpRatioBuy: number;
  tpRatioSell: number;
  tpFixedBuy: number;
  tpFixedSell: number;
  tpAmpBuy?: number;
  tpAmpSell?: number;
  tpEnabled?: boolean; // 新增：是否开启止盈挂单
  tpBuyEnabled?: boolean;
  tpSellEnabled?: boolean;
  slModeBuy: 'ratio' | 'fixed' | 'amp';
  slModeSell: 'ratio' | 'fixed' | 'amp';
  slRatioBuy: number;
  slRatioSell: number;
  slFixedBuy: number;
  slFixedSell: number;
  slAmpBuy?: number;
  slAmpSell?: number;
  slEnabled?: boolean; // 新增：是否开启止损挂单
  slBuyEnabled?: boolean;
  slSellEnabled?: boolean;
  mLinkEnabled?: boolean; // 新增：关联M生效
  mLinkValue?: number; // 新增：关联M值
  positiveWindow: number;
  maxHoldTime: number;
  kBestPeriod: string;
  kBestWindow: [number, number];
}

export interface EmailSettings {
  enabled: boolean;
  sender: string;
  receiver: string;
  smtpServer: string;
  smtpPort: number;
  password: string;
  minBalance: number;
  maxConsecutiveLoss: number;
}

export interface WithdrawalSettings {
  withdrawalThreshold: number;
  retentionThreshold: number;
  alarmThreshold: number;
}

export interface AccountConfig {
  id: string;
  name: string;
  enabled: boolean;
  isMasterAccount?: boolean; // 新增：指定为主账户
  binance: BinanceSettings;
  scanner: ScannerSettings;
  order: OrderSettings;
  withdrawal: WithdrawalSettings;
}

export interface AppSettings {
  appName?: string;
  accounts: AccountConfig[];
  email: EmailSettings;
  auth: {
    username: string;
    password: string;
  };
}

export interface TradeLog {
  id: string;
  symbol: string;
  side: 'BUY' | 'SELL';
  leverage: number;
  amount: number;
  entryPrice: number;
  exitPrice: number;
  pnl: number;
  fee: number;
  fundingFee: number;
  fundingFeeCheckedCount?: number; // 新增：记录资金费查询次数
  fundingRate?: number; // 新增：当前资金费率
  profitRate: number;
  kBestChange?: number; // 新增：K优涨跌幅
  amp?: number; // 新增：振幅
  mValue?: number; // 新增：M值
  realA?: number; // 新增：真实A
  openTime: number;
  closeTime: number;
  status: 'OPEN' | 'CLOSED';
}

export interface LogEntry {
  id: string;
  timestamp: number;
  type: 'info' | 'success' | 'warning' | 'error';
  module: string;
  message: string;
  details?: any;
}

export interface Position {
  symbol: string;
  amount: number;
  entryPrice: number;
  side: 'BUY' | 'SELL';
  timestamp: number;
}

export interface Order {
  symbol: string;
  orderId: number;
  type: string;
  side: string;
  price: string;
  stopPrice?: string;
  timestamp: number;
}

export interface TransferLog {
  id: string;
  asset: string;
  amount: number;
  type: 'IN' | 'OUT'; // IN: Spot to Futures, OUT: Futures to Spot
  status: 'SUCCESS' | 'FAILED';
  timestamp: number;
  message?: string;
}

export interface BalanceLog {
  id: string;
  totalBalance: number;
  timestamp: number;
}

export interface Kline {
  symbol: string;
  interval: string;
  openTime: number;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
  closeTime: number;
  quoteAssetVolume: number;
  numberOfTrades: number;
  takerBuyBaseAssetVolume: number;
  takerBuyQuoteAssetVolume: number;
  change?: number; // 新增：涨跌幅
  amplitude?: number; // 新增：振幅
}
