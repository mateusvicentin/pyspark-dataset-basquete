<h1 align="center">PySpark com Dataset da NBA 1997-2023</h1>
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

<h2>df_player</h2>

```python
df_player = spark.read.csv("C:\\Users\\Vicentin\\Documents\\Estudos\\Dados\\CSV\\traditional.csv", encoding='utf-8', header=True, inferSchema=True, sep=',')
```
<p>Para ler os arquivos, é necessário informar o caminho onde eles estão armazenados. Como os arquivos estão baixados na minha máquina, eu forneço o caminho do arquivo e informo o encoding <code>UTF-8</code>, caso haja algum caractere especial.</p>

