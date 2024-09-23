import random
import psycopg2

# Параметры подключения к базам данных
master_connection_params = {'host': '127.0.0.2',
        'port': '5432',
        'database': 'testdbmaster',
        'user': 'postgres',
        'password': '123'}
replica1_connection_params = {'host': '127.0.0.3',
        'port': '5432',
        'database': 'testdbreplica1',
        'user': 'postgres',
        'password': '123'
}
replica2_connection_params = {'host': '127.0.0.4',
        'port': '5432',
        'database': 'testdbreplica2',
        'user': 'postgres',
        'password': '123'
}
replica3_connection_params = {'host': '127.0.0.5',
        'port': '5432',
        'database': 'testdbreplica3',
        'user': 'postgres',
        'password': '123'
}



# Таблицы фрагментов данных на узлах
fragment0_rows = []
fragment1_rows = [2, 5, 8, 12, 20, 22, 23, 25, 31, 32, 34, 43, 44, 46, 48, 55, 57, 59, 61, 62, 64, 65, 71, 75, 77, 79, 85, 86, 87, 88, 95, 100, 106, 113, 117, 121, 123, 125, 126, 127, 132, 133, 135, 136, 137, 138, 140, 142, 144, 145, 146]
fragment2_rows = [1, 7, 11, 13, 15, 16, 17, 19, 21, 24, 27, 29, 33, 35, 36, 39, 41, 45, 47, 50, 52, 54, 58, 67, 69, 76, 78, 82, 83, 84, 89, 90, 92, 93, 96, 97, 98, 99, 101, 102, 103, 105, 107, 108, 109, 114, 118, 119, 120, 124, 128, 130, 131, 134, 139, 141, 143, 147, 150]
fragment3_rows = [3, 4, 6, 9, 10, 14, 18, 26, 28, 30, 37, 38, 40, 42, 49, 51, 53, 56, 60, 63, 66, 68, 70, 72, 73, 74, 80, 81, 91, 94, 104, 110, 111, 112, 115, 116, 122, 129, 148, 149]


# Функция генетического алгоритма
def genetic_algorithm():
    population_size = 10
    generations = 100
    mutation_rate = 0.1

    # Генерация начальной популяции
    population = []
    for _ in range(population_size):
        chromosome = random.sample(range(4), 3)
        population.append(chromosome)

    # Цикл по поколениям
    for _ in range(generations):
        # Вычисление приспособленности для каждой особи в популяции
        fitness_scores = []
        for chromosome in population:
            fitness_scores.append(fitness_function(chromosome))

        # Выбор лучших особей для скрещивания
        mating_pool = []
        for _ in range(population_size // 2):
            parent1 = select_parent(population, fitness_scores)
            parent2 = select_parent(population, fitness_scores)
            mating_pool.append((parent1, parent2))

        # Скрещивание и мутация
        new_population = []
        for parents in mating_pool:
            offspring1, offspring2 = crossover(parents)
            offspring1 = mutate(offspring1, mutation_rate)
            offspring2 = mutate(offspring2, mutation_rate)
            new_population.append(offspring1)
            new_population.append(offspring2)

        population = new_population

    # Определение лучшей особи
    best_chromosome = max(population, key=lambda chromosome: fitness_function(chromosome))

    # Извлечение строк из мастер таблицы
    rows = extract_rows_from_master()

    # Вставка строк в соответствующие таблицы на узлах
    insert_rows(best_chromosome, rows)
    print("Всё успешно добавлено")


# Параметры для вычисления задержки доступа
access_time_per_row = 0.001
master_delay_factor = 1.0
replica_delay_factor = 0.8

# Подключение к базе данных
def connect_to_database(connection_params):
    try:
        conn = psycopg2.connect(**connection_params)
        return conn
    except psycopg2.Error as e:
        print(f"Error connecting to the database: {e}")
        return None

# Функция получения данных с узла
def fetch_data_from_node(conn, fragment):
    if fragment == 0:
        return fragment0_rows
    elif fragment == 1:
        return fragment1_rows
    elif fragment == 2:
        return fragment2_rows
    elif fragment == 3:
        return fragment3_rows

# Функция расчета задержки доступа
def calculate_access_delay(chromosome):
    delay = 0.0
    conn_master = connect_to_database(master_connection_params)
    conn_replica1 = connect_to_database(replica1_connection_params)
    conn_replica2 = connect_to_database(replica2_connection_params)
    conn_replica3 = connect_to_database(replica3_connection_params)

    if conn_master:
        with conn_master:
            cursor = conn_master.cursor()
            for node in chromosome:
                if node == 0:
                    delay += master_delay_factor * len(fetch_data_from_node(conn_master, node))
                elif node == 1:
                    delay += replica_delay_factor * len(fetch_data_from_node(conn_replica1, node))
                elif node == 2:
                    delay += replica_delay_factor * len(fetch_data_from_node(conn_replica2, node))
                elif node == 3:
                    delay += replica_delay_factor * len(fetch_data_from_node(conn_replica3, node))
        conn_master.close()

    return delay * access_time_per_row

# Функция расчета веса задержки доступа в зависимости от базы данных на узлах
def calculate_delay_weight(chromosome):
    delay_weight = 0.5

    if 0 in chromosome:
        delay_weight *= 1.2

    if 1 in chromosome or 2 in chromosome or 3 in chromosome:
        delay_weight *= 0.8

    return delay_weight

# Функция приспособленности
def fitness_function(chromosome):
    if len(set(chromosome)) != len(chromosome):
        return float('-inf')

    data_balance = len(set(chromosome))
    access_delay = calculate_access_delay(chromosome)
    delay_weight = calculate_delay_weight(chromosome)

    fitness = data_balance + delay_weight * access_delay
    return fitness

# Тестирование функции приспособленности
chromosome = [0, 1, 2, 3]
fitness = fitness_function(chromosome)
print(fitness)
# Выбор родителя
def select_parent(population, fitness_scores):
    total_fitness = sum(fitness_scores)
    probabilities = [score / total_fitness for score in fitness_scores]
    parent_index = random.choices(range(len(population)), probabilities)[0]
    return population[parent_index]

# Скрещивание
def crossover(parents):
    parent1, parent2 = parents
    crossover_point = random.randint(1, len(parent1) - 1)
    offspring1 = parent1[:crossover_point] + parent2[crossover_point:]
    offspring2 = parent2[:crossover_point] + parent1[crossover_point:]
    return offspring1, offspring2

# Мутация
def mutate(chromosome, mutation_rate):
    if random.random() < mutation_rate:
        gene_index = random.randint(0, len(chromosome) - 1)
        chromosome[gene_index] = random.randint(0, 3)
    return chromosome

# Вставка строк в таблицы на узлах
def insert_rows(chromosome, rows):
    for i, fragment_rows in enumerate([fragment1_rows, fragment2_rows, fragment3_rows]):
        if fragment_rows:
            node_index = chromosome[i]
            if node_index == 0:
                connection_param = master_connection_params
            elif node_index == 1:
                connection_param = replica1_connection_params
            elif node_index == 2:
                connection_param = replica2_connection_params
            elif node_index == 3:
                connection_param = replica3_connection_params

            # Подключение к базе данных
            conn = psycopg2.connect(**connection_param)
            cursor = conn.cursor()

            # Вставка строк в таблицу фрагмента
            for row_number in fragment_rows:
                row = rows[row_number - 1]  # Индексы начинаются с 0, номера строк - с 1
                insert_query = "INSERT INTO book (isbn, title, genre, publicationyear, publisherid) VALUES (%s, %s, %s, %s, %s)"
                cursor.execute(insert_query, (row[0], row[1], row[2], row[3], row[4]))

            # Закрытие соединений
            conn.commit()
            cursor.close()
            conn.close()

# Извлечение строк из мастер таблицы
def extract_rows_from_master():
    master_conn = psycopg2.connect(**master_connection_params)
    master_cursor = master_conn.cursor()
    master_cursor.execute('SELECT * FROM Book')
    rows = master_cursor.fetchall()
    master_cursor.close()
    master_conn.close()
    return rows

# Запуск генетического алгоритма
genetic_algorithm()
