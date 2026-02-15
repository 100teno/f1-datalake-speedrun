# %%
import pandas as pd
import fastf1

pd.set_option("display.max_rows", 500)
pd.set_option("display.max_columns", 500)
# %%

#  Criar uma classe para carregar os dados
class Loader:
    def __init__(self, start, stop, identifiers):
        self.start = start
        self.stop = stop
        self.identifiers = identifiers

# Criar um método para carregar os dados, onde o usuário pode escolher o ano, o gp e o identificador (qualificação, corrida, treino livre 1, treino livre 2, treino livre 3)
    def get_data(self, year, gp, identifier):

        try:
            session = fastf1.get_session(year, gp, identifier)
            session.load(
                laps=False,
                telemetry=False,
                weather=False,
                messages=False,
            )
            df = session.results
            df['identifier'] = identifier
            df['date'] = session.date
            df['year'] = session.date.year
            df['RoundNumber'] = session.event['RoundNumber']
            df['Country'] = session.event['Country']
            df['Location'] = session.event['Location']
            df['OfficialEventName'] = session.event['OfficialEventName']
            return df
        except ValueError as err:
            print(err)
            print("Erro capturado")
            return pd.DataFrame()
        
    def save_data(self, year,  gp, identifier, df):
        pass

# %%

loader = Loader(2025, 1, ['race', 'sprint'])

df = loader.get_data(2025, 1, 'race')
df

# %%
session = fastf1.get_session(2025, 1, 'race')
session.load(
    laps=False,
    telemetry=False,
    weather=False,
    messages=False,
)
# %%