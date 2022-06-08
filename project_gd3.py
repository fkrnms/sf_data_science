import pandas as pd
from pandas.core.tools.datetimes import to_datetime
events1 = pd.read_csv(r'C:\Users\liuba\Documents\data\7_4_Events.csv', sep=',')
purchase1 = pd.read_csv(r'C:\Users\liuba\Documents\data\purchase.csv', sep=',')

#преобразуем данные: находим строки, относящиеся к событиям до 2018 года и удаляем их, оставляем строки с пользователями, 
#зарегестрированными в 2018 году, преобразуем типы данных для дат, обьеденяем полученные таблицы, переименовав столбцы
events1 = events1.sort_values('start_time')
#print(events1[events1['start_time'].str.contains('2018-')].head(1))
#найдя индекс 1ой строки,  в которой есть 2018, удаляем все предшевствующие строки
events2 = events1.drop(events1.index[0:51405])

purchase1 = purchase1.sort_values('event_datetime')
#print(purchase1[purchase1['event_datetime'].str.contains('2018-')].head(1))
#то же делаем с таблицей 2
purchase2 = purchase1.drop(purchase1.index[0:1164])

events2['start_time'] = pd.to_datetime(events2['start_time'])
purchase2['event_datetime'] = pd.to_datetime(purchase2['event_datetime'])

mask_reg2018 = events2[(events2['event_type'] == 'registration') & (events2['start_time'].dt.year == 2018)]['user_id']
#фильтруем таблицы по маске с user_id, зарегестрировавшихся в 2018
events = events2.merge(mask_reg2018, on='user_id', how='inner')
purchase = purchase2.merge(mask_reg2018, on='user_id', how='inner')
#добавляем столбец с типом события "покупка", чтобы различать его в объедененной таблице
purchase['event_type'] = 'purchase'
#переименовываем столбцы с различной информацией, но одинаковым названиям в исходных таблицах
events = events.rename(columns={'id': 'event_id'})
purchase = purchase.rename(columns={'id': 'purchase_id'})
#объединяем таблицы, обновляем индексы
total_events = pd.concat([events,purchase],sort=False)
total_events = total_events.reset_index(drop=True).sort_values('start_time')

# выделяем группы пользователей в зависимости от уровня сложности(easy, medium, hard), считаем их долю от всех пользователей
users_with_easy_l = total_events[total_events['selected_level'] == 'easy']['user_id'].unique()
users_with_medium_l = total_events[total_events['selected_level'] == 'medium']['user_id'].unique()
users_with_hard_l = total_events[total_events['selected_level'] == 'hard']['user_id'].unique()

percent_easy_l = len(users_with_easy_l) / total_events['user_id'].nunique()
percent_medium_l = len(users_with_medium_l) / total_events['user_id'].nunique()
percent_hard_l = len(users_with_hard_l) / total_events['user_id'].nunique()

print("доля пользователей с easy уровнем: {:.2%}".format(percent_easy_l))
print("доля пользователей с medium уровнем: {:.2%}".format(percent_medium_l))
print("доля пользователей с hard уровнем: {:.2%}".format(percent_hard_l))

# расчитывем процент оплат для групп пользователей с разным уровнем сложности
purchase_easy_l = purchase[purchase['user_id'].isin(users_with_easy_l)]
purchase_medium_l = purchase[purchase['user_id'].isin(users_with_medium_l)]
purchase_hard_l = purchase[purchase['user_id'].isin(users_with_hard_l)]

percent_purchase_easy_l = purchase_easy_l['user_id'].nunique() / len(users_with_easy_l)
percent_purchase_medium_l = purchase_medium_l['user_id'].nunique() / len(users_with_medium_l)
percent_purchase_hard_l = purchase_hard_l['user_id'].nunique() / len(users_with_hard_l)

print("доля оплативших пользователей с easy уровнем : {:.2%}".format(percent_purchase_easy_l))
print("доля оплативших пользователей с medium уровнем: {:.2%}".format(percent_purchase_medium_l))
print("доля оплативших пользователей с hard уровнем: {:.2%}".format(percent_purchase_hard_l))

#опрелеяем,различается ли временной промежуток между регистрацией и первой(!) оплатой у групп пользователей с разным уровнем сложности
print('Среднее время от регистрации до оплаты для пользователей:')
registration = total_events[total_events['event_type'] == 'registration'] #таблица с регистрациями
registration_easy_l = registration[registration['user_id'].isin(users_with_easy_l)] #талица с регистрациями из группы easy  уровня
registration_easy_l = registration_easy_l[['user_id', 'start_time']] #id и время регистрации для группы easy уровня
first_purchase_easy_l = purchase_easy_l.sort_values('event_datetime').drop_duplicates('user_id')  #оставляем события по 1ой оплате
first_purchase_easy_l = first_purchase_easy_l[['user_id', 'event_datetime']]  #id и время оплаты для группы easy уровня
merged_easy_l = registration_easy_l.merge(first_purchase_easy_l, on='user_id', how='inner') #объединяем таблицы с регистрациями и оплатой
merged_easy_l['timedelta'] = (merged_easy_l['event_datetime'] - merged_easy_l['start_time']) #считаем временную разницу между оплатой и регистрацией
easy_l_time = merged_easy_l['timedelta'].mean() # находим среднее время
print('-с easy уровнем ', easy_l_time)

registration_medium_l = registration[registration['user_id'].isin(users_with_medium_l)] #все то же делаем для других групп
registration_medium_l = registration_medium_l[['user_id', 'start_time']]
first_purchase_medium_l = purchase_medium_l.sort_values('event_datetime').drop_duplicates('user_id')
first_purchase_medium_l = first_purchase_medium_l[['user_id', 'event_datetime']]
merged_medium_l = registration_medium_l.merge(first_purchase_medium_l, on='user_id', how='inner')
merged_medium_l['timedelta'] = (merged_medium_l['event_datetime'] - merged_medium_l['start_time'])
medium_l_time = merged_medium_l['timedelta'].mean()
print('-с medium уровнем ', medium_l_time)

registration_hard_l = registration[registration['user_id'].isin(users_with_hard_l)]
registration_hard_l = registration_hard_l[['user_id', 'start_time']]
first_purchase_hard_l = purchase_hard_l.sort_values('event_datetime').drop_duplicates('user_id')
first_purchase_hard_l = first_purchase_hard_l[['user_id', 'event_datetime']]
merged_hard_l = registration_hard_l.merge(first_purchase_hard_l, on='user_id', how='inner')
merged_hard_l['timedelta'] = (merged_hard_l['event_datetime'] - merged_hard_l['start_time'])
hard_l_time = merged_hard_l['timedelta'].mean()
print('-с hard уровнем ', hard_l_time)

#опрелеяем,различается ли временной промежуток между выбором уровня сложности и первой(!) оплатой у групп пользователей с разным уровнем сложности
#считаем по аналогии с регистрацией/оплатой, вместо registration, используем событие level_choice
print('Среднее время от выбора уровня сложности до оплаты для пользователей:')
level_choice = total_events[total_events['event_type'] == 'level_choice']
level_choice_easy_l = level_choice[level_choice['user_id'].isin(users_with_easy_l)]
level_choice_easy_l = level_choice_easy_l[['user_id', 'start_time']]
merged_easy_l_2 = level_choice_easy_l.merge(first_purchase_easy_l, on='user_id', how='inner')
merged_easy_l_2['timedelta'] = (merged_easy_l_2['event_datetime'] - merged_easy_l_2['start_time'])
easy_l_time_2 = merged_easy_l_2['timedelta'].mean()
print('-с easy уровнем ', easy_l_time_2)

level_choice_medium_l = level_choice[level_choice['user_id'].isin(users_with_medium_l)]
level_choice_medium_l = level_choice_medium_l[['user_id', 'start_time']]
merged_medium_l_2 = level_choice_medium_l.merge(first_purchase_medium_l, on='user_id', how='inner')
merged_medium_l_2['timedelta'] = (merged_medium_l_2['event_datetime'] - merged_medium_l_2['start_time'])
medium_l_time_2 = merged_medium_l_2['timedelta'].mean()
print('-с medium уровнем ', medium_l_time_2)

level_choice_hard_l = level_choice[level_choice['user_id'].isin(users_with_hard_l)]
level_choice_hard_l = level_choice_hard_l[['user_id', 'start_time']]
merged_hard_l_2 = level_choice_hard_l.merge(first_purchase_hard_l, on='user_id', how='inner')
merged_hard_l_2['timedelta'] = (merged_hard_l_2['event_datetime'] - merged_hard_l_2['start_time'])
hard_l_time_2 = merged_hard_l_2['timedelta'].mean()
print('-с hard уровнем ', hard_l_time_2)

