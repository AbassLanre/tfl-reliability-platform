


with ranked as (
  select
    tr.trades_id,
    tr.amount,
    tr.trade_date,
    ts.trader_id,
    ts.name,
    ts.desk,
    row_number() over (order by amount desc) as rn
    from trades as tr 
    left join traders as ts 
    on tr.trader_id = ts.trader_id
)

select

r.desk, 
r.amount

from ranked as r
where amount > 1000000


def mostOccurring(lists):
  stringSet = {}
  for s in lists:
    if s not in stringSet:
      stringSet[s] =1
    else:
      stringSet[s] +=1
  
  sortedList = sorted(stringSet.items(), key = lambda x:x[1], reverse= true)
  return sortedList[0][0]