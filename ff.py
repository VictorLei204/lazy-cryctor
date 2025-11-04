import pandas as pd
import os,copy,math,perf
import warnings
warnings.filterwarnings('ignore')

path = '/home/wangs/data/'
basic_path = '/home/wangs/data/basic/'
factor_path = '/home/wangs/data/factor/'
cl = pd.read_pickle(basic_path + 'cl.pk')
idt = pd.read_pickle(basic_path + 'idt.pk')
if_trade = pd.read_pickle(basic_path + 'if_trade.pk')
if_new = pd.read_pickle(basic_path + 'if_new.pk')
if_st = pd.read_pickle(basic_path + 'if_st.pk')
up = pd.read_pickle(basic_path + 'up.pk')
down = pd.read_pickle(basic_path + 'down.pk')
filter0 = if_trade * if_new * if_st
filter1 = if_trade * if_new * if_st * up * down


def read_min(code):
    df = pd.read_pickle('/home/wangs/data/mins/'+code+'.pk')
    df.index = df.index.map(str)
    return df

def read(name,type = 'basic'):
     if type == 'basic':
        df = pd.read_csv(basic_path + name + '.csv',index_col=0)
        return df
     else:
        df = pd.read_hdf('/home/wangs/data/'+ type + '//' + name+'.h5')
        return df

closes = read('close','factor').ffill(axis = 1)
open = read('open','factor').ffill(axis = 1)
rets_all = read('close','factor').pct_change(1,axis = 1) * filter0
    
def save(name, df, type='factor'):
    df.to_hdf(path + type + '//' + name + '.h5', key='data')

def delay(date, p=1):
     return idt[idt.index(date)+p]

def trans_code(code):
    if code[0] == '6':
        return code + '.SH'
    else:
        return code + '.SZ'
def cal_downdraw(cumsum):
    downdraw=[]
    for i in range(len(cumsum)):
        lst=cumsum.iloc[:i+1]
        a=lst.max()
        b=cumsum.iloc[i]
        downdraw.append(a-b)
    return -pd.Series(downdraw).max()

def cal_returns(returns):
    result={}
    cumsum=returns.cumsum()
    result['年化收益率']=cumsum.iloc[-1]/len(cumsum)*250
    result['年化波动率']=(returns).std()*(250**0.5)
    result['夏普率']=result['年化收益率']/result['年化波动率']
    result['最大回撤']=cal_downdraw((returns).cumsum())
    result['收益回撤比']=-result['年化收益率']/result['最大回撤']
    result['胜率']=round(len(returns[returns>0])/len(returns),3)
    result['盈亏比']=-returns[returns>0].mean()/returns[returns<=0].mean()
    return result

import numpy as np
def log_return(arith_return):
    return np.log(1 + arith_return)

def exp_return(log_return):
    return np.exp(log_return) - 1

def rolling_window(a, window):
    a = np.array(a)
    shape = a.shape[:-1] + (a.shape[-1] - window + 1, window)
    strides = a.strides + (a.strides[-1],)
    return np.lib.stride_tricks.as_strided(a, shape=shape, strides=strides)


from loguru import logger
import traceback,datetime
import time

logger.add("/home/wangs/log/"+str(datetime.date.today())+".log", rotation="500MB", encoding="utf-8", enqueue=True, compression="zip")

def log_function(func):
    def wrapper(*args, **kwargs):
        try:
            start_time = time.time()
            result = func(*args, **kwargs)
            end_time = time.time()
            execution_time = end_time - start_time
            logger.info(os.getcwd() + f" Function {func.__name__} executed successfully in {execution_time:.4f} seconds")
            return result
        except Exception as e:
            error = traceback.format_exc()
            logger.error(os.getcwd() + f" Error executing function {func.__name__}: {e}" + '/n' + error)
    return wrapper





### jg.run


### backtest 旧仓位回测框架
### 回测框架 - 固定周期、100%换仓/分层回测
## drop

def check_buy(code_list,date,amount):
    is_trade = if_trade[date].dropna()
    is_trade = is_trade[is_trade==1].index.tolist()
    st = if_st[date].dropna()
    st = st[st==1].index.tolist()

    ret = rets_all[date].dropna()
    up = ret[ret>0.097].index.tolist()
    down = ret[ret<-0.097].index.tolist()
    available = list(set(code_list) & set(is_trade)&set(st) - set(up) - set(down) )
    buys = pd.Series(code_list)
    buys = buys[buys.isin(available)].iloc[:amount].tolist()
    return buys


class hold(object):
    def __init__(self,factors,positions=100,ascending=True,period=10,holding_types='normal',groups=(10,10)):
        self.factors = factors
        self.positions = positions
        self.ascending = ascending
        self.period = period
        self.groups = groups
        self.holding_types=holding_types
        
    def info(self):
        if self.holding_types=='normal':
            print('持仓模式： 普通模式')
        else:
            print('持仓模式：',self.holding_types+'分层；',str(self.groups[0])+'层,每组'+str(self.groups[1])+'个')
        print('持仓周期：',self.period)
        print('仓位数：  ',self.positions)
        if self.ascending:
            print('排序:      正序')
        else:
            print('排序：     倒序')
            
            
    def normal_holdings(self):
        holdings={}
        for i in self.factors.columns[:]:
            if len(self.factors[i].dropna())==0:
                continue
            df=self.factors[i].dropna().sort_values(ascending=self.ascending).index.tolist()
            holdings[i]=check_buy(df,i,self.positions)
        holdings=get_holding(holdings,self.period)
        return holdings
    
    def layer_holdings(self):
        print('因子分层：\n分层数据—'+self.holding_types+'.csv')
        layer=pd.read_csv('e://脚本//数据//'+self.holding_types+'.csv')
        layer=layer.set_index('code')
        layer.index=layer.index.map(lambda x:str(x).zfill(6))
        print('数据加载完毕，正在分层')
        dates=self.factors.columns.tolist()
        holdings={}
        for i in dates:
            if len(self.factors[i].dropna())==0:
                continue
            if len(layer[i].dropna())==0:
                print(i)
                continue
            a=pd.DataFrame(pd.qcut(layer[i].dropna().rank(method='first'),self.groups[0],labels=False))
            a['factor']=self.factors[i]    
            holds=a.groupby([i]).apply(lambda x:x.sort_values(by='factor',ascending=self.ascending).index.tolist()).tolist()
            h=[]
            for n in holds:
                n=check_buy(n,i,self.groups[1])
                h.extend(n)
            holdings[i]=h
        holdings=get_holding(holdings,self.period)
        print('分层持仓生成完毕')
        return holdings    
    def layer_holdings_mutilate(self):
        print('因子分层：\n分层数据—'+self.holding_types+'.csv')
        layer=pd.read_csv('e://脚本//数据//'+self.layer+'.csv')
        if self.layer=='mkv' or self.layer=='l_mkv':
            layer = layer.iloc[:,966:]
        layer=layer.set_index('code')
        layer.index=layer.index.map(lambda x:str(x).zfill(6))
        print('数据加载完毕，正在分层')
        dates=self.factors.columns.tolist()
        holdings={}
        for i in dates:
            if len(self.factors[i].dropna())==0:
                continue
            if len(layer[i].dropna())==0:
                print(i)
                continue
            a=pd.DataFrame(pd.qcut(layer[i].dropna().rank(method='first'),self.groups[0],labels=False))
            a['factor']=self.factors[i]    
            holds=a.groupby([i]).apply(lambda x:x.sort_values(by='factor',ascending=self.ascending).index.tolist()).tolist()
            h=[]
            for n in holds:#根据组数，改变取值的起始点再循环
                n=check_buy(n,i,self.groups[1])
                h.extend(n)
            holdings[i]=h
        holdings=get_holding(holdings,self.period)
        print('分层持仓生成完毕')
        return holdings
def get_holding(holdings,frequent):
    positions_dates=sorted(list(holdings.keys()))
    holds={}
    change_dates=[]
    for i in range(0,len(positions_dates),frequent):
        start=positions_dates[i]
        change_dates.append(start)
        if i+frequent<len(positions_dates):
            end=positions_dates[i+frequent]
        else:
            end=''
        a=0
        for n in positions_dates:
            if n == start:
                a=1
            if n == end:
                a=0
            if a == 1:
                code_list=holdings[start]
                holds[n]=code_list
    return holds#,change_dates


class backtest(object):
    def __init__(self, holdings, positions=100,types='t-close',fees=0.002):
        self.result = holdings#[0]
        self.change_dates = sorted(holdings)#holdings[1]
        self.nums = positions
        self.types = types
        self.fees = fees
        self.dates  = sorted(holdings)#sorted(holdings[0].keys())
        
    def info(self):
        print('回测仓位数：',self.nums)
        print('回测手续费：',self.fees)
        if self.types == 'open':
            print('交易模式： 开盘价买入，收盘价卖出')
        elif self.types == 't-close':
            print('交易模式： t日收盘价买入，收盘价卖出')
        else:
            print('交易模式： t+1日收盘价买入，收盘价卖出')

    def ini_position(self):
        ini={}
        ini['状态']=[True]*self.nums
        ini['持仓']=[0]*self.nums
        ini['净值']=[1/self.nums]*self.nums
        ini['汇总']=[0,1,0]
        infos = {}
        for i in self.result.keys():
            infos[i]=ini
        self.position = infos

    def update_sell(self,sell_index,date):
        code = self.position[date]['持仓'][sell_index]
        jingzhi = self.position[date]['净值'][sell_index]
        #print('sell:',code,date)
        close = closes.loc[code,date]
        pre_close = closes.loc[code,idt[idt.index(date)-1]]
        ret = (close - pre_close -close*self.fees)/(pre_close)
        if math.isnan(ret):
            print(code,date,ret)
        return jingzhi*(1+ret)

    def update_hold(self,hold_index,date):
        code = self.position[date]['持仓'][hold_index]
        jingzhi = self.position[date]['净值'][hold_index]
        
        close = closes.loc[code,date]
        pre_close = closes.loc[code,idt[idt.index(date)-1]]
        ret = (close - pre_close)/(pre_close)
        
        if math.isnan(ret):
            print(code,date,ret)

        return jingzhi*(1+ret)

    def update_buy(self,empty_index,real_buy,date,money):
        if self.types == 't-close' or self.types == 'close':
            if date == self.change_dates[0]:
                jingzhi = money / (1 + self.fees)
            else:
                jingzhi = money * (1 - self.fees)
        if self.types == 'open':
            buy_price = opens.loc[real_buy,date]
            end_price = closes.loc[real_buy,date]
            jingzhi = money*(1+(end_price - buy_price -buy_price*self.fees)/((1+self.fees)*buy_price))
        return jingzhi
    
    def check_sell(self, code_list, date):
        #sell_list=[]
        return code_list,[]       

    def check_position(self, date):
        chicang=copy.deepcopy(self.position[date]['持仓'])
        zhuangtai=copy.deepcopy(self.position[date]['状态'])
        jingzhi=copy.deepcopy(self.position[date]['净值'])

        def get_trade_info():
            '''
            返回当日持仓位置，卖出位置和空仓位置
            '''
            empty_index=list(filter(lambda x:chicang[x]==0,range(self.nums)))
            holding = list(filter(lambda x:chicang[x]!=0,range(self.nums)))
            holding_codes = list(map(lambda x:chicang[x],holding))
            if self.types=='t-close':
                aim_holding = self.result[date][:self.nums]#获取目标持仓
            else:
                rank = self.dates.index(date)
                if rank == 0:
                    aim_holding = []
                else:
                    aim_holding = self.result[self.dates[rank-1]][:self.nums]

            #print(len(aim_holding),date)
            positive_holding_codes = list(set(holding_codes) & set(aim_holding))#获取主动继续持仓标的代码
            positive_holding = list(map(lambda x:chicang.index(x),positive_holding_codes))
            #获取被动继续持仓标和平仓标的的代码
            #aim_holding_codes = list(map(lambda x:chicang[x],holding))
            sells, negative_holding = self.check_sell(list(set(holding_codes) - set(aim_holding)), date)
            negative_holding = list(map(lambda x:chicang.index(x),negative_holding))
            
            sells_index=[]
            if len(sells)!=0:
                for i in range(len(sells)):
                    sells_index.append(chicang.index(sells[i]))

            #实际当日买入标的列表
            if self.types=='t-close':
                empty_index.extend(sells_index)

            real_buy = list(set(aim_holding)-set(positive_holding_codes))[:self.nums][:len(empty_index)]
            return positive_holding,sells_index,empty_index,real_buy
        
        positive_holding,sells_index,empty_index,real_buy=get_trade_info()

        #更新卖出仓位
        for i in range(len(sells_index)):
            chicang[sells_index[i]] = 0 #对建仓的仓位进行更新
            jingzhi[sells_index[i]] = self.update_sell(sells_index[i],date)#还未清零，只有没股票才清零，当日收盘买入会更更新，当日收盘没买入会清零
            zhuangtai[sells_index[i]] = True 


        #更新主动持仓仓位
        for i in range(len(positive_holding)):
            jingzhi[positive_holding[i]]=self.update_hold(positive_holding[i],date)

        if self.types == 'open':
            close_sell_cash = sum(list(map(lambda x:jingzhi[x],sells_index)))
            cash = list(filter(lambda x:chicang[x]==0,range(len(chicang))))
            cash = sum(list(map(lambda x:jingzhi[x],cash)))
            available_cash = cash - close_sell_cash
        else:
            cash = list(filter(lambda x:chicang[x]==0,range(len(chicang))))
            cash = sum(list(map(lambda x:jingzhi[x],cash)))
            available_cash = cash 


        zongjingzhi = sum(list(map(lambda x:jingzhi[x],range(len(chicang)))))

        if date in self.change_dates :#调仓日，先建新仓，再平衡持仓的仓位
            money = (zongjingzhi)/self.nums
            for i in range(len(empty_index)):#开新仓
                if i < len(real_buy):
                    chicang[empty_index[i]] = real_buy[i]
                    jingzhi[empty_index[i]] = self.update_buy(empty_index[i],real_buy[i],date,money)
                    zhuangtai[empty_index[i]] = False
                else:
                    chicang[empty_index[i]] = 0
                    jingzhi[empty_index[i]] = money
                    zhuangtai[empty_index[i]] = True

            for i in range(len(positive_holding)):
                old_jingzhi = jingzhi[positive_holding[i]]
                f = abs(old_jingzhi-money)*self.fees
                jingzhi[positive_holding[i]] = money - f


        elif len(empty_index)!=0:#非调仓日且有空余仓位，根据现金建新仓
            money = available_cash/len(empty_index)
            for i in range(len(empty_index)):
                if i <len(real_buy):
                    chicang[empty_index[i]] = real_buy[i]
                    jingzhi[empty_index[i]] = self.update_buy(empty_index[i],real_buy[i],date,money)
                    zhuangtai[empty_index[i]] = False
                else:
                    chicang[empty_index[i]] = 0
                    jingzhi[empty_index[i]] = money
                    zhuangtai[empty_index[i]] = True
        else:
            money = 0

        chicangjingzhi = list(filter(lambda x:chicang[x]!=0,range(self.nums)))
        chicangjingzhi = sum(list(map(lambda x:jingzhi[x],chicangjingzhi)))

        xianjin = list(filter(lambda x:chicang[x]==0,range(len(chicang))))
        xianjin = sum(list(map(lambda x:jingzhi[x],xianjin)))
        
        result={}
        result['状态']=zhuangtai
        result['净值']=jingzhi
        result['持仓']=chicang
        result['汇总']=[chicangjingzhi,xianjin,len(list(filter(lambda x:x != 0,chicang)))]
        return result

    def cal(self):
        summary={}
        mvs=[]
        mv_dates=[]
        date_range = sorted(list(self.result.keys()))[:]
        for i in date_range[:]:
            s = self.check_position(i)
            summary[i] = copy.deepcopy(s)
            mvs.append(copy.deepcopy(s['汇总'][0]+s['汇总'][1]))  
            if i != date_range[-1]:     
                self.position[idt[idt.index(i) + 1]] = s
        #print(self.position)
        return summary,mvs,date_range


def run(factor,types='t-close',positions=100,period=20,ascending=True,fees=0.002,group_num=6,all='long',holding_types='normal',draw=True,bench='all'):
    print(all,holding_types)
    if all=='all':
        print('暂停使用')
        return
#         IC=cal_IC(factor,holdings[1])[1]
#         draw_IC(IC)
    elif all=='long':
        holding=hold(factor,positions=positions,period=period,ascending=ascending,holding_types=holding_types)
        holding.info()
        if holding_types=='normal':
            holdings=holding.normal_holdings()
        else:
            holdings=holding.layer_holdings()

        print('回测部分：')
        a=backtest(holdings,positions,fees=fees,types=types)
        a.info()
        a.ini_position()
        mv=a.cal()
        r=perf.compare_draw(mv[1],mv[2],draw=draw,bench=bench)
        display(r[0])
        parames = tuple((fees,period,positions,ascending,bench))
        r = r + parames
        return r
    else:
        print('error')