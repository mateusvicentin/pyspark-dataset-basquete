<p align="center">
  <img src="https://github.com/mateusvicentin/pyspark-dataset-basquete/assets/31457038/a25168fd-a6ba-4cea-b4dc-846544ca7e44" alt="img0">
</p>


<h1 align="center">PySpark com Dataset da NBA 1997-2023 e Jogos Internacionais de Futebol 1872-2024</h1>

# Índice 
* [NBA 1997-2023](#NBA-1997-2023)
* [Jogos Internacionais de Futebol 1872-2024](#Jogos-Internacionais-de-Futebol-1872-2024)

</br>
</br>
</br>

<h1>NBA 1997-2023</h1>
<p>Neste projeto, utilizarei um dataset fornecido pelo site da Kaggle, que contém informações sobre times e jogadores da NBA, abrangendo jogos de 1997 até 2023.</p>
<p>Link para o dataset: <a href="https://www.kaggle.com/datasets/szymonjwiak/nba-traditional">NBA Traditional Boxscores 1997-2023</a></p>
<h2>Sobre o Dataset</h2>
<p>Os box scores de jogadores e times das temporadas da NBA de 1996-97 a 2022-23 foram extraídos do site NBA.com. O dataset inclui:</p>
<ul>
  <li>702.387 box scores de jogadores</li>
  <li>65.574 box scores de times</li>
  <li>31.856 jogos de temporada regular</li>
  <li>2.189 jogos de playoff</li>
  <li>19 jogos de play-in</li>
</ul>
<p>As temporadas são especificadas pelo ano em que terminaram; por exemplo, a temporada 2006-2007 é descrita como 2007.</p>
<p>Os dados foram extraídos em janeiro de 2024.</p>
<p>Os box scores de times para as temporadas de 1997, 1998, 1999, 2000, 2006 e 2010 foram compilados a partir dos box scores de jogadores devido a razões técnicas.</p>
<p>***Informações retiradas do proprio Dataset***</p>

<p>São dois datasets fornecidos:</p>
<ul>
  <li><code>team_traditional.csv</code> - times</li>
  <li><code>traditional.csv</code> - jogadores</li>
</ul>

<h2>Carregando os Dados</h2>
<p>Após baixar os arquivos, utilizarei o VS Code com o Spark para carregar as informações e começar a realizar os tratamentos e verificações dos dados.</p>

```python
import findspark
from pyspark.sql.functions import count, col, asc, desc, sum, concat, to_date, year, month, max
findspark.init()
```
```python
from pyspark.sql import SparkSession
spark = SparkSession.builder\
        .master('local')\
        .appName('nbastats')\
        .getOrCreate()
```
<p>Após iniciar a sessão do Spark, farei a leitura dos arquivos. Para isso, criarei dois DataFrames: um chamado <code>df_player</code> e outro chamado <code>df_team</code>, cada um deles lendo o seu respectivo arquivo.</p>

<h4>df_player</h4>

```python
df_player = spark.read.csv("C:\\Users\\Vicentin\\Documents\\Estudos\\Dados\\CSV\\traditional.csv", encoding='utf-8', header=True, inferSchema=True, sep=',')
```
<p>Para ler os arquivos, é necessário informar o caminho onde eles estão armazenados. Como os arquivos estão baixados na minha máquina, eu forneço o caminho do arquivo e informo o encoding <code>UTF-8</code>, caso haja algum caractere especial.</p>

<h4>df_team</h4>

```python
df_team = spark.read.csv("C:\\Users\\Vicentin\\Documents\\Estudos\\Dados\\CSV\\team_traditional.csv",encoding='utf-8', header=True, inferSchema=True, sep=',')
```
<p>Para ler os arquivos, é necessário informar o caminho onde eles estão armazenados. Como os arquivos estão baixados na minha máquina, eu forneço o caminho do arquivo e informo o encoding <code>UTF-8</code>, caso haja algum caractere especial.</p>
<p>É utilizado o parâmetro <code>sep=','</code> em ambos Dataframes porque, no arquivo CSV, a vírgula é o separador das informações. Dessa forma, o Spark consegue ler o arquivo corretamente sem se confundir ou ter problemas para retornar as informações.</p>

<h2>Verificando os Esquemas dos DataFrames</h2>
<p>Para este procedimento, é utilizado o comando <code>&lt;nomedodataframe&gt;.printSchema()</code>. No caso, será utilizado <code>df_player.printSchema()</code> e <code>df_team.printSchema()</code>, pois os DataFrames <code>df_player</code> e <code>df_team</code> foram criados anteriormente.</p>

```python
df_player.printSchema()
```
<p align="center">
  <img src="https://github.com/mateusvicentin/pyspark-dataset-basquete/assets/31457038/8b9adc2b-8f8a-4dfd-a2a6-8ae3ebef8aaa" alt="img1">
</p>

```python
df_team.printSchema()
```
<p align="center">
  <img src="https://github.com/mateusvicentin/pyspark-dataset-basquete/assets/31457038/c8ddd550-d4b7-492b-9a8f-baa7ec2351a9" alt="img2">
</p>
<p>Podemos verificar como as tabelas estão estruturadas, qual é o tipo de cada coluna, se é uma String, uma Data ou um número inteiro, por exemplo.</p>

<h2>Visualizando Tabela</h2>
<p>Agora, vamos visualizar as informações que serão trazidas do dataframe. Vou utilizar o <code>df_player</code> como exemplo.</p>

```python
df_player.show(truncate=False)
```
<p>É utilizado o parâmetro <code>truncate=False</code> para exibir os dados de forma que fiquem alinhados e completos, sem cortar as informações.</p>
<p align="center">
  <img src="https://github.com/mateusvicentin/pyspark-dataset-basquete/assets/31457038/8d0f67a2-134d-4291-adf4-542790673654" alt="img3">
</p>
<p>Nesse caso, quero selecionar apenas algumas colunas: gameid, date, player, team, home, away, MIN e PTS</p>
<p>Irei alterar o nome de duas colunas; neste caso, MIN para minutes_played e PTS para pts_player.</p>

<p>Vou criar também mais 2 colunas. Será utilizado como base a coluna <code>date</code> para criar uma coluna com o mês (<code>month</code>) e outra com o ano (<code>year</code>), utilizando as informações presentes em <code>date</code>.</p>

```python
df_player = df_player.withColumn('pts_player', df_player['PTS']).drop('PTS')
df_player = df_player.withColumn('minutes_played', df_player['MIN']).drop('MIN')
df_player_filter = df_player.select('gameid', 'date', 'player', 'team', 'home', 'away', 
                                    'minutes_played', 'pts_player')

df_player_filter = df_player_filter.withColumn('month', month(df_player_filter['date']))
df_player_filter = df_player_filter.withColumn('year', year(df_player_filter['date']))
df_player_filter.show(truncate=False)
```
<p align="center">
  <img src="https://github.com/mateusvicentin/pyspark-dataset-basquete/assets/31457038/ffd55efd-1ce7-4af6-82f9-09f11c08f492" alt="img4">
</p>

<p>Podemos observar que ele está fornecendo muito menos informações, e podemos verificar que as colunas 'month' e 'year' estão em conformidade com as informações mostradas na coluna 'date'.</p>
<p>Vamos fazer o mesmo para o DataFrame <code>df_team</code>. Desta vez, iremos selecionar apenas as colunas 'gameid', 'date', 'team', 'away' e 'PTS'. Assim como foi feito para o <code>df_player</code>, vamos renomear 'PTS' para 'pts_team' e criar as colunas 'month' (mês) e 'year' (ano) para o DataFrame <code>df_team</code>.</p>

```python
df_team_filter = df_team.select('gameid', 'date', 'team', 'away',  'PTS')
df_team_filter = df_team_filter.withColumn('pts_team', df_team_filter['PTS']).drop('PTS')
df_team_filter = df_team_filter.withColumn('month', month(df_team_filter['date']))
df_team_filter = df_team_filter.withColumn('year', year(df_team_filter['date']))
df_team_filter.show(truncate=False)
```
<p align="center">
  <img src="https://github.com/mateusvicentin/pyspark-dataset-basquete/assets/31457038/21e7cd11-9e94-4766-8ee6-affbbfe503c1" alt="img5">
</p>
<h2>Realizando Consultas</h2>
<p>Vou realizar uma consulta para trazer de forma decrescente os pontos da equipe ('pts_team') do dataframe df_team.</p>

```python
df_desc_team = df_team_filter.orderBy(col('pts_team').desc())
df_desc_team.show(truncate=False)
```
<p align="center">
  <img src="https://github.com/mateusvicentin/pyspark-dataset-basquete/assets/31457038/0a18137d-b1b1-45e4-9424-a4ea89b030b7" alt="img6">
</p>
<p>Vou fazer o mesmo com o dataframe df_player.</p>

```python
df_desc_player = df_player_filter.orderBy(col('pts_player').desc())
df_desc_player.show(truncate=False)
```
<p align="center">
  <img src="https://github.com/mateusvicentin/pyspark-dataset-basquete/assets/31457038/b93025ed-c8c5-4fad-824f-8000c1aa6ba8" alt="img7">
</p>
<p>Segundo os dados do arquivo baixado, no DataFrame df_team, o time SAC (Sacramento Kings) fez um total de 176 pontos em uma partida realizada em 23/02/2023. No DataFrame df_player, o jogador Kobe Bryant fez 81 pontos em uma partida no dia 22/01/2006.</p>

<p>Vou realizar uma consulta para verificar quantas partidas cada jogador teve.</p>

```python
df_jogos_player = df_player.groupBy('player').count().orderBy(col('count').desc()) 
df_jogos_player.show(truncate=False)
```
<p align="center">
  <img src="https://github.com/mateusvicentin/pyspark-dataset-basquete/assets/31457038/5e049c2f-caee-4e39-a4b3-18151d7010f0" alt="img8">
</p>

<p>Agora, vou realizar a soma de todos os pontos de cada jogador.</p>

```python
df_sum_player = df_player_filter.groupBy('player').agg(sum('pts_player')).orderBy(col('sum(pts_player)').desc())
df_sum_player.show(truncate=False)
```
<p align="center">
  <img src="https://github.com/mateusvicentin/pyspark-dataset-basquete/assets/31457038/8c0c1dc2-f99f-4f22-859e-22f2f6f24c27" alt="img9">
</p>
<p>Agora, vou fazer o mesmo somando os pontos das equipes para verificar a equipe que mais tem ponto somado.</p>

```python
df_sum_team = df_team_filter.groupBy('team').agg(sum('pts_team')).orderBy(col('sum(pts_team)').desc())
df_sum_team.show(truncate=False)
```
<p align="center">
  <img src="https://github.com/mateusvicentin/pyspark-dataset-basquete/assets/31457038/0a0cf6f2-19fe-491e-900f-bf118c85cd0c" alt="img10">
</p>
<p>Nessa execução, é mostrado que o jogador LeBron James tem um total de 46,727 pontos, e o time que tem mais pontos somados é o LAL (Los Angeles Lakers) com um total de 240,421 pontos.</p>

<h1>Jogos Internacionais de Futebol 1872-2024</h1>
<p>Neste projeto, utilizarei um dataset fornecido pelo site da Kaggle, que contém informações resultados de jogos internacionais de futebol de 1872-2024.</p>
<p>Link para o dataset: <a href="[https://www.kaggle.com/datasets/szymonjwiak/nba-traditional](https://www.kaggle.com/datasets/martj42/international-football-results-from-1872-to-2017)">International football results from 1872 to 2024</a></p>
<h2>Sobre o Dataset</h2>
<p>Foi utilizado o arquivo <em>goalscorers.csv</em> que inclui as seguintes colunas:</p>
<ul>
  <li><strong>date</strong>: data da partida</li>
  <li><strong>home_team</strong>: nome do time da casa</li>
  <li><strong>away_team</strong>: nome do time visitante</li>
  <li><strong>team</strong>: nome do time que marcou o gol</li>
  <li><strong>scorer</strong>: nome do jogador que marcou o gol</li>
  <li><strong>own_goal</strong>: se o gol foi um gol contra</li>
  <li><strong>penalty</strong>: se o gol foi marcado de pênalti</li>
</ul>

<h2>Carregando os Dados</h2>
<p>Após baixar o arquivo, utilizarei o VS Code com o Spark para carregar as informações e começar a realizar os tratamentos e verificações dos dados.</p>

```python
import findspark
from pyspark.sql.functions import count, col, sum,  year, month, max, avg
findspark.init()
```
```python
from pyspark.sql import SparkSession
spark = SparkSession.builder\
        .master('local')\
        .appName('fotball')\
        .getOrCreate()
```
<h4>goalscorers.csv</h4>

```python
df = spark.read.csv('C:\\Users\\Vicentin\\Documents\\Estudos\\Dados\\CSV\\goalscorers.csv', encoding='utf-8', header=True, inferSchema=True, sep=',')
df.show()
```
<p align="center">
  <img src="https://github.com/mateusvicentin/pyspark-dataset-basquete/assets/31457038/c43a66e0-a775-4e9d-b24b-5b56c1595cdc" alt="img11">
</p>
<p>Nesse dataframe, da mesma forma que foi feito no da NBA, irei criar duas colunas chamadas <em>month</em> (mês) e <em>year</em> (ano).</p>

```python
df = df.withColumn('month', month(df['date']))
df = df.withColumn('year', year(df['date']))
df.show(truncate=False)
```
<p align="center">
  <img src="https://github.com/mateusvicentin/pyspark-dataset-basquete/assets/31457038/b8d34956-c5f9-4677-92e9-f0225e3fc8a0" alt="img12">
</p>
<p>Vou fazer a contagem dos jogadores que mais marcaram gols.</p>

```python
df_player_most_goals = df.groupBy('scorer').count().orderBy(col('count').desc())
df_player_most_goals.show(truncate=False)
```
<p align="center">
  <img src="https://github.com/mateusvicentin/pyspark-dataset-basquete/assets/31457038/388abdb4-a1ca-4298-877c-bd4b50576084" alt="img13">
</p>
<p>Vou fazer a contagem das seleções que mais fizeram gols.</p>

```python
df_team_most_goals = df.groupBy('team').count().orderBy(col('count').desc())
df_team_most_goals.show(truncate=False)
```
<p align="center">
  <img src="https://github.com/mateusvicentin/pyspark-dataset-basquete/assets/31457038/ab62d3c2-f00f-4922-af59-d4094526d906" alt="img14">
</p>
<p>Agora vou filtrar quem fez mais gols pela seleção, no caso, quero puxar apenas da seleção brasileira. Para isso, vou usar o método .filter.</p>

```python
df_brazil = df.filter(df.team.isin('Brazil')).groupBy('scorer').count().orderBy(col('count').desc())
df_brazil.show(truncate=False)
```
<p align="center">
  <img src="https://github.com/mateusvicentin/pyspark-dataset-basquete/assets/31457038/cd6dcf5a-8220-4ab7-86bd-bde89591c518" alt="img15">
</p>

<p>Vou aproveitar e filtrar a quantidade de gols que um jogador fez pela seleção brasileira em determinado ano.</p>

```python
df_brazil = df.filter(df.team.isin('Brazil')).groupBy('year','scorer').count().orderBy(col('count').desc())
df_brazil.show(truncate=False)
```
<p align="center">
  <img src="https://github.com/mateusvicentin/pyspark-dataset-basquete/assets/31457038/250c15e4-4077-4025-a32d-2ca83916c6e0" alt="img16">
</p>

<p>Vou realizar uma consulta para verificar quantos gols no total foram feitos de pênalti e quantos não foram.</p>

```python
df_penalti = df.groupBy('penalty').count().orderBy(col('count').desc())
df_penalti.show(truncate=False)
```
<p align="center">
  <img src="https://github.com/mateusvicentin/pyspark-dataset-basquete/assets/31457038/f4daed15-d029-4ae3-b28b-296d6ac9e4f2" alt="img17">
</p>
<p>Somando a quantidade total de gols feitos pelas seleções.</p>

```python
df_total_gols = df.groupBy('scorer').count()
df_total_gols.agg(sum('count')).alias('qtd_gols').show(truncate=False)
```
<p align="center">
  <img src="https://github.com/mateusvicentin/pyspark-dataset-basquete/assets/31457038/11018ab5-f7e1-48f3-bc01-90a8e6546278" alt="img18">
</p>
<p>Encerro aqui esse projeto utilizando o PySpark do Spark para algumas consultas de Datasets, não foi feito nenhum tipo de tratamento apenas realização de filtros para consulta dos dados informados.</p>






