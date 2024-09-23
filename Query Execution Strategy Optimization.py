import random
import psycopg2

# Задание констант и параметров генетического алгоритма
POPULATION_SIZE = 50
GENERATIONS = 1
MUTATION_RATE = 0.1

# Задание структуры данных для представления стратегии исполнения запроса
GENES_PER_FRAGMENT = 3

# Задание структуры данных для представления популяции
class Individual:
    def __init__(self, genes):
        self.genes = genes
        self.fitness = 0

# Функция для выполнения запроса на узле PostgreSQL
def execute_query_on_node(node, fragment, join_operation):
    conn = psycopg2.connect(
        host=node["host"],
        port=node["port"],
        database=node["database"],
        user=node["user"],
        password=node["password"]
    )
    cursor = conn.cursor()
    if fragment['table'] == 'book':
        cursor.execute(f"SELECT * FROM {fragment['table']};")
    else:
        cursor.execute(f"SELECT * FROM {fragment['table']} {join_operation} book ON {fragment['table']}.isbn = book.isbn")
    result = cursor.fetchall()
    cursor.close()
    conn.close()
    return random.uniform(0, 1)

# Подключение к базе данных PostgreSQL
def connect_to_database(node):
    return {
        "host": f"127.0.0.{node}",
        "port": 5432,
        "database": f"testdbreplica{node}",
        "user": "postgres",
        "password": "123"
    }

# Функция для выбора фрагмента таблицы
def choose_fragment(node, table):
    return {
        "host": f"127.0.0.{node}",
        "port": 5432,
        "database": f"testdbreplica{node}",
        "user": "postgres",
        "password": "123",
        "table": table
    }

# Функция для выбора операции соединения на основе объема данных и стоимости операций
def choose_join_operation(volume, cost):
    if volume >= 1000 and cost <= 10:
        return "JOIN"
    elif volume >= 1000 and cost > 10:
        return "MERGE JOIN"
    else:
        return "HASH JOIN"

# Функция для получения имени таблицы по индексу
def get_table_name(index):
    return "book"

# Генерация случайной популяции
def generate_population(size):
    population = []
    for _ in range(size):
        genes = [random.randint(1, 2) for _ in range(GENES_PER_FRAGMENT)]  # Установить гены только для таблицы "book"
        individual = Individual(genes)
        population.append(individual)
    return population

# Вычисление пригодности (fitness) каждой особи в популяции
def calculate_fitness(population):
    fitness_values = []
    for individual in population:
        fitness = 0
        for node_index, gene in enumerate(individual.genes):
            node = connect_to_database(node_index + 1)
            fragment = choose_fragment(node_index + 1, get_table_name(gene))
            volume = calculate_data_volume(node, fragment)
            cost = calculate_operation_cost(fragment)
            join_operation = choose_join_operation(volume, cost)
            fitness += execute_query_on_node(node, fragment, join_operation)
        individual.fitness = fitness
        fitness_values.append(fitness)
    return fitness_values

# Функция для вычисления объема данных фрагмента таблицы
def calculate_data_volume(node, fragment):
    conn = psycopg2.connect(
        host=node["host"],
        port=node["port"],
        database=node["database"],
        user=node["user"],
        password=node["password"]
    )
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {fragment['table']};")
    result = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    return result

# Функция для вычисления стоимости операции соединения
def calculate_operation_cost(fragment):
    conn = psycopg2.connect(
        host=fragment["host"],
        port=fragment["port"],
        database=fragment["database"],
        user=fragment["user"],
        password=fragment["password"]
    )
    cursor = conn.cursor()
    cursor.execute(f"EXPLAIN (FORMAT JSON) SELECT * FROM {fragment['table']} LIMIT 1;")
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    cost = 0
    if result:
        plan = result[0]
        if "Plan" in plan and "Total Cost" in plan["Plan"]:
            cost = plan["Plan"]["Total Cost"]
    return cost

# Операция скрещивания (однородное скрещивание)
def crossover(parent1, parent2):
    child_genes = []
    for gene1, gene2 in zip(parent1.genes, parent2.genes):
        if random.random() < 0.5:
            child_genes.append(gene1)
        else:
            child_genes.append(gene2)
    return Individual(child_genes)

# Операция мутации (замена случайного гена)
def mutate(individual):
    gene_index = random.randint(0, GENES_PER_FRAGMENT - 1)
    individual.genes[gene_index] = random.randint(1, 2)  # Мутировать только гены для таблицы "book"

# Выбор лучших особей (турнирная селекция)
def selection(population):
    selected = []
    for _ in range(len(population)):
        tournament = random.sample(population, min(3, len(population)))
        tournament.sort(key=lambda x: x.fitness, reverse=True)
        selected.append(tournament[0])
    return selected

# Генетический алгоритм для оптимизации стратегии исполнения запроса
def genetic_algorithm():
    population = generate_population(POPULATION_SIZE)
    best_individual = None  # Переменная для хранения лучшей особи
    for _ in range(GENERATIONS):
        calculate_fitness(population)
        population.sort(key=lambda x: x.fitness, reverse=True)
        if len(population) > 0 and population[0].fitness == 1.0:
            best_individual = population[0]
            break
        selected = selection(population)
        offspring = []
        for i in range(0, len(selected) - 1, 2):
            parent1 = selected[i]
            parent2 = selected[i + 1]
            child = crossover(parent1, parent2)
            offspring.append(child)
        for individual in offspring:
            if random.random() < MUTATION_RATE:
                mutate(individual)
        population = offspring
    if best_individual is None and len(population) > 0:
        best_individual = max(population, key=lambda x: x.fitness)
    return best_individual

# Запуск генетического алгоритма и вывод результата
best_strategy = genetic_algorithm()
print("Лучшая стратегия:", best_strategy.genes)
