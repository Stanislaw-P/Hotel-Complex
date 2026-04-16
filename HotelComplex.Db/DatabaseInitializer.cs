using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using System.Data;

namespace HotelComplex.Db
{
    public class DatabaseInitializer
    {
        private readonly string _connectionString;
        private readonly ILogger<DatabaseInitializer> _logger;

        public DatabaseInitializer(IConfiguration configuration, ILogger<DatabaseInitializer> logger)
        {
            _connectionString = configuration.GetConnectionString("DefaultConnection")
                ?? throw new InvalidOperationException("Connection string not found");
            _logger = logger;
        }

        public async Task InitializeAsync()
        {
            try
            {
                // Проверяем подключение к MySQL
                await TestConnectionAsync();

                // Создаем базу данных и таблицы
                await CreateDatabaseAndTablesAsync();

                _logger.LogInformation("Database initialized successfully");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to initialize database");
                throw;
            }
        }

        private async Task TestConnectionAsync()
        {
            using var connection = new MySqlConnection(_connectionString);
            await connection.OpenAsync();
            _logger.LogInformation("Successfully connected to MySQL server");
        }

        private async Task CreateDatabaseAndTablesAsync()
        {
            using var connection = new MySqlConnection(_connectionString);
            await connection.OpenAsync();

            // Создаем базу данных, если не существует
            await ExecuteCommandAsync(connection, "CREATE DATABASE IF NOT EXISTS HotelComplex;");

            // Переключаемся на созданную базу
            await connection.ChangeDatabaseAsync("HotelComplex");

            // Создаем таблицу журнала ПЕРВОЙ
            await CreateJournalTable(connection);

            // Создаем все основные таблицы
            await CreateRoomTypeTable(connection);
            await CreateRoomTable(connection);
            await CreateGuestTable(connection);
            await CreateEmployeeTable(connection);
            await CreateCorporatePartnerTable(connection);
            await CreateContractTable(connection);
            await CreateServiceTable(connection);
            await CreateBookingTable(connection);
            await CreateStayTable(connection);
            await CreateServiceOrderTable(connection);
            await CreateInvoiceTable(connection);
            await CreateReviewTable(connection);

            // Добавляем начальные данные
            await InsertInitialData(connection);

            // Удаляем старые архивные таблицы (если есть)
            await DropArchiveTables(connection);

            // Создаем триггеры и архивные таблицы для всех основных таблиц
            await CreateTriggersForAllTables(connection);

            _logger.LogInformation("All tables, archive tables, and triggers created successfully");
        }

        private async Task ExecuteCommandAsync(MySqlConnection connection, string commandText)
        {
            try
            {
                using var cmd = new MySqlCommand(commandText, connection);
                await cmd.ExecuteNonQueryAsync();
                _logger.LogDebug("Executed SQL command: {Command}", commandText[..Math.Min(50, commandText.Length)]);
            }
            catch (MySqlException ex)
            {
                // Игнорируем ошибки duplicate key и already exists
                if (ex.Number != 1061 && ex.Number != 1050 && ex.Number != 1062)
                {
                    _logger.LogWarning(ex, "Error executing SQL command: {Command}", commandText);
                }
            }
        }

        private async Task CreateRoomTypeTable(MySqlConnection connection)
        {
            var sql = @"
                CREATE TABLE IF NOT EXISTS RoomType (
                    Id SMALLINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                    Name VARCHAR(50) NOT NULL,
                    Description TEXT,
                    UNIQUE KEY Unique_Name (Name)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;";

            await ExecuteCommandAsync(connection, sql);
        }

        private async Task CreateRoomTable(MySqlConnection connection)
        {
            var sql = @"
                CREATE TABLE IF NOT EXISTS Room (
                    Id SMALLINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                    RoomNumber SMALLINT UNSIGNED NOT NULL,
                    Floor TINYINT UNSIGNED NOT NULL,
                    RoomTypeId SMALLINT UNSIGNED NOT NULL,
                    Capacity TINYINT UNSIGNED NOT NULL,
                    BasePrice DECIMAL(10,2) NOT NULL,
                    Status ENUM('free', 'occupied', 'cleaning', 'repair') DEFAULT 'free',
                    UNIQUE KEY Unique_RoomNumber (RoomNumber),
                    INDEX idx_Status (Status),
                    INDEX idx_RoomTypeId (RoomTypeId),
                    FOREIGN KEY (RoomTypeId) REFERENCES RoomType(Id) ON DELETE RESTRICT
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;";

            await ExecuteCommandAsync(connection, sql);
        }

        private async Task CreateGuestTable(MySqlConnection connection)
        {
            var sql = @"
                CREATE TABLE IF NOT EXISTS Guest (
                    Id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                    LastName VARCHAR(50) NOT NULL,
                    FirstName VARCHAR(50) NOT NULL,
                    MiddleName VARCHAR(50),
                    PassportSeries VARCHAR(10) NOT NULL,
                    PassportNumber VARCHAR(20) NOT NULL,
                    Citizenship VARCHAR(50) DEFAULT 'РФ',
                    Phone VARCHAR(20) NOT NULL,
                    Email VARCHAR(100),
                    UNIQUE KEY Unique_Passport (PassportSeries, PassportNumber),
                    UNIQUE KEY Unique_Phone (Phone),
                    INDEX idx_Email (Email),
                    INDEX idx_Name (LastName, FirstName)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;";

            await ExecuteCommandAsync(connection, sql);
        }

        private async Task CreateEmployeeTable(MySqlConnection connection)
        {
            var sql = @"
                CREATE TABLE IF NOT EXISTS Employee (
                    Id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                    LastName VARCHAR(50) NOT NULL,
                    FirstName VARCHAR(50) NOT NULL,
                    MiddleName VARCHAR(50),
                    PassportSeries VARCHAR(10) NOT NULL,
                    PassportNumber VARCHAR(20) NOT NULL,
                    Position VARCHAR(25) NOT NULL,
                    Phone VARCHAR(20) NOT NULL,
                    Email VARCHAR(100),
                    UNIQUE KEY Unique_Passport (PassportSeries, PassportNumber),
                    UNIQUE KEY Unique_Phone (Phone),
                    UNIQUE KEY Unique_Email (Email)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;";

            await ExecuteCommandAsync(connection, sql);
        }

        private async Task CreateCorporatePartnerTable(MySqlConnection connection)
        {
            var sql = @"
                CREATE TABLE IF NOT EXISTS CorporatePartner (
                    Id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                    OrganizationName VARCHAR(200) NOT NULL,
                    LegalAddress VARCHAR(300) NOT NULL,
                    ContactPerson VARCHAR(150) NOT NULL,
                    Phone VARCHAR(20) NOT NULL,
                    Email VARCHAR(100) NOT NULL,
                    UNIQUE KEY Unique_OrganizationName (OrganizationName),
                    INDEX idx_Phone (Phone)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;";

            await ExecuteCommandAsync(connection, sql);
        }

        private async Task CreateContractTable(MySqlConnection connection)
        {
            var sql = @"
                CREATE TABLE IF NOT EXISTS Contract (
                    Id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                    PartnerId INT UNSIGNED NOT NULL,
                    ConclusionDate DATE NOT NULL,
                    ValidUntil DATE NOT NULL,
                    DiscountRate DECIMAL(5,2) DEFAULT 0.00,
                    INDEX idx_ValidUntil (ValidUntil),
                    FOREIGN KEY (PartnerId) REFERENCES CorporatePartner(Id) ON DELETE RESTRICT
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;";

            await ExecuteCommandAsync(connection, sql);
        }

        private async Task CreateServiceTable(MySqlConnection connection)
        {
            var sql = @"
                CREATE TABLE IF NOT EXISTS Service (
                    Id SMALLINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                    Name VARCHAR(100) NOT NULL,
                    Price DECIMAL(10,2) NOT NULL,
                    UNIQUE KEY Unique_Name (Name)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;";

            await ExecuteCommandAsync(connection, sql);
        }

        private async Task CreateBookingTable(MySqlConnection connection)
        {
            var sql = @"
                CREATE TABLE IF NOT EXISTS Booking (
                    Id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                    GuestId INT UNSIGNED NOT NULL,
                    RoomId SMALLINT UNSIGNED NOT NULL,
                    PartnerId INT UNSIGNED NULL,
                    ContractId INT UNSIGNED NULL,
                    CheckInDate DATE NOT NULL,
                    CheckOutDate DATE NOT NULL,
                    Status ENUM('confirmed', 'cancelled', 'completed') DEFAULT 'confirmed',
                    Prepayment DECIMAL(10,2) DEFAULT 0.00,
                    INDEX idx_GuestId (GuestId),
                    INDEX idx_RoomId (RoomId),
                    INDEX idx_Dates (CheckInDate, CheckOutDate),
                    INDEX idx_Status (Status),
                    FOREIGN KEY (GuestId) REFERENCES Guest(Id) ON DELETE RESTRICT,
                    FOREIGN KEY (RoomId) REFERENCES Room(Id) ON DELETE RESTRICT,
                    FOREIGN KEY (PartnerId) REFERENCES CorporatePartner(Id) ON DELETE SET NULL,
                    FOREIGN KEY (ContractId) REFERENCES Contract(Id) ON DELETE SET NULL
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;";

            await ExecuteCommandAsync(connection, sql);
        }

        private async Task CreateStayTable(MySqlConnection connection)
        {
            var sql = @"
                CREATE TABLE IF NOT EXISTS Stay (
                    Id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                    BookingId INT UNSIGNED NOT NULL,
                    GuestId INT UNSIGNED NOT NULL,
                    RoomId SMALLINT UNSIGNED NOT NULL,
                    ActualCheckIn DATETIME NOT NULL,
                    ActualCheckOut DATETIME NULL,
                    TotalAmount DECIMAL(10,2) DEFAULT 0.00,
                    UNIQUE KEY Unique_Booking (BookingId),
                    INDEX idx_GuestId (GuestId),
                    INDEX idx_RoomId (RoomId),
                    INDEX idx_Dates (ActualCheckIn, ActualCheckOut),
                    FOREIGN KEY (BookingId) REFERENCES Booking(Id) ON DELETE RESTRICT,
                    FOREIGN KEY (GuestId) REFERENCES Guest(Id) ON DELETE RESTRICT,
                    FOREIGN KEY (RoomId) REFERENCES Room(Id) ON DELETE RESTRICT
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;";

            await ExecuteCommandAsync(connection, sql);
        }

        private async Task CreateServiceOrderTable(MySqlConnection connection)
        {
            var sql = @"
                CREATE TABLE IF NOT EXISTS ServiceOrder (
                    Id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                    ServiceId SMALLINT UNSIGNED NOT NULL,
                    StayId INT UNSIGNED NOT NULL,
                    OrderDate DATETIME NOT NULL,
                    Status ENUM('completed', 'cancelled') DEFAULT 'completed',
                    INDEX idx_OrderDate (OrderDate),
                    FOREIGN KEY (ServiceId) REFERENCES Service(Id) ON DELETE RESTRICT,
                    FOREIGN KEY (StayId) REFERENCES Stay(Id) ON DELETE RESTRICT
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;";

            await ExecuteCommandAsync(connection, sql);
        }

        private async Task CreateInvoiceTable(MySqlConnection connection)
        {
            var sql = @"
                CREATE TABLE IF NOT EXISTS Invoice (
                    Id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                    StayId INT UNSIGNED NOT NULL,
                    GuestId INT UNSIGNED NOT NULL,
                    EmployeeId INT UNSIGNED NOT NULL,
                    InvoiceDate DATETIME NOT NULL,
                    Amount DECIMAL(10,2) NOT NULL,
                    IsPaid BOOLEAN DEFAULT FALSE,
                    UNIQUE KEY Unique_Stay (StayId),
                    FOREIGN KEY (StayId) REFERENCES Stay(Id) ON DELETE RESTRICT,
                    FOREIGN KEY (GuestId) REFERENCES Guest(Id) ON DELETE RESTRICT,
                    FOREIGN KEY (EmployeeId) REFERENCES Employee(Id) ON DELETE RESTRICT
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;";

            await ExecuteCommandAsync(connection, sql);
        }

        private async Task CreateReviewTable(MySqlConnection connection)
        {
            var sql = @"
                CREATE TABLE IF NOT EXISTS Review (
                    Id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                    GuestId INT UNSIGNED NOT NULL,
                    StayId INT UNSIGNED NOT NULL,
                    FeedbackText TEXT NOT NULL,
                    FeedbackDate DATETIME NOT NULL,
                    Type ENUM('review', 'complaint') NOT NULL,
                    INDEX idx_Type (Type),
                    FOREIGN KEY (GuestId) REFERENCES Guest(Id) ON DELETE RESTRICT,
                    FOREIGN KEY (StayId) REFERENCES Stay(Id) ON DELETE RESTRICT
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;";

            await ExecuteCommandAsync(connection, sql);
        }

        private async Task InsertInitialData(MySqlConnection connection)
        {
            // Вставляем типы номеров
            var roomTypes = @"
                INSERT IGNORE INTO RoomType (Name, Description) VALUES
                ('Стандарт', 'Стандартный номер с одной кроватью'),
                ('Полулюкс', 'Улучшенный номер с дополнительным пространством'),
                ('Люкс', 'Номер повышенной комфортности'),
                ('Апартаменты', 'Просторные апартаменты с гостиной зоной');";

            await ExecuteCommandAsync(connection, roomTypes);

            // Вставляем услуги
            var services = @"
                INSERT IGNORE INTO Service (Name, Price) VALUES
                ('Завтрак', 500),
                ('Обед', 800),
                ('Ужин', 1000),
                ('Прачечная', 300),
                ('Химчистка', 500),
                ('Трансфер', 1500),
                ('Экскурсия', 2000),
                ('СПА', 2500),
                ('Тренажерный зал', 500),
                ('Парковка', 300);";

            await ExecuteCommandAsync(connection, services);

            // Генерируем номера
            await GenerateRooms(connection);
        }

        private async Task GenerateRooms(MySqlConnection connection)
        {
            // Получаем ID типов номеров
            var getTypeIds = "SELECT Id FROM RoomType ORDER BY Id";
            List<ushort> typeIds = new List<ushort>();

            using (var cmd = new MySqlCommand(getTypeIds, connection))
            using (var reader = await cmd.ExecuteReaderAsync())
            {
                while (await reader.ReadAsync())
                {
                    typeIds.Add(Convert.ToUInt16(reader[0]));
                }
            }

            if (typeIds.Count == 0) return;

            // Генерируем номера для 5 этажей
            for (int floor = 1; floor <= 5; floor++)
            {
                for (int roomNum = 1; roomNum <= 10; roomNum++)
                {
                    ushort roomNumber = (ushort)(floor * 100 + roomNum);
                    ushort roomTypeId = typeIds[(roomNum - 1) % typeIds.Count];
                    byte capacity = (byte)((roomNum % 3) + 1);
                    decimal basePrice = 2000 + (floor * 500) + (roomNum * 100);

                    var insertRoom = @"
                        INSERT IGNORE INTO Room (RoomNumber, Floor, RoomTypeId, Capacity, BasePrice, Status)
                        VALUES (@RoomNumber, @Floor, @RoomTypeId, @Capacity, @BasePrice, 'free');";

                    using var cmd = new MySqlCommand(insertRoom, connection);
                    cmd.Parameters.AddWithValue("@RoomNumber", roomNumber);
                    cmd.Parameters.AddWithValue("@Floor", floor);
                    cmd.Parameters.AddWithValue("@RoomTypeId", roomTypeId);
                    cmd.Parameters.AddWithValue("@Capacity", capacity);
                    cmd.Parameters.AddWithValue("@BasePrice", basePrice);

                    await cmd.ExecuteNonQueryAsync();
                }
            }

            _logger.LogInformation("Generated rooms for all floors");
        }


        private async Task CreateJournalTable(MySqlConnection connection)
        {
            var sql = @"
        CREATE TABLE IF NOT EXISTS journal (
            Id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
            TableName VARCHAR(100) NOT NULL DEFAULT '',
            Operation VARCHAR(10) NOT NULL DEFAULT '',
            OperationDate DATETIME NOT NULL,
            UserName VARCHAR(200) NOT NULL DEFAULT '',
            INDEX idx_TableName (TableName),
            INDEX idx_Operation (Operation),
            INDEX idx_OperationDate (OperationDate),
            INDEX idx_UserName (UserName)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;";

            await ExecuteCommandAsync(connection, sql);
            _logger.LogInformation("Journal table created");
        }

        private async Task DropArchiveTables(MySqlConnection connection)
        {
            var sql = "SHOW TABLES";
            var tables = new List<string>();

            using (var cmd = new MySqlCommand(sql, connection))
            using (var reader = await cmd.ExecuteReaderAsync())
            {
                while (await reader.ReadAsync())
                {
                    var tableName = reader.GetString(0);
                    if (tableName.Contains("_arch"))
                    {
                        tables.Add(tableName);
                    }
                }
            }

            foreach (var table in tables)
            {
                try
                {
                    await ExecuteCommandAsync(connection, $"DROP TABLE IF EXISTS {table}");
                    _logger.LogDebug($"Dropped archive table: {table}");
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, $"Failed to drop archive table: {table}");
                }
            }
        }

        private async Task CreateArchiveTable(MySqlConnection connection, string tableName)
        {
            var archiveName = $"{tableName}_arch";

            // Создаем архивную таблицу с такой же структурой
            var createArchiveSql = $"CREATE TABLE IF NOT EXISTS {archiveName} LIKE {tableName}";
            await ExecuteCommandAsync(connection, createArchiveSql);

            // Добавляем связь с журналом
            var addJournalIdSql = $@"
        ALTER TABLE {archiveName} 
        ADD COLUMN JournalId INT UNSIGNED,
        ADD INDEX idx_JournalId (JournalId),
        ADD FOREIGN KEY (JournalId) REFERENCES journal(Id) ON DELETE SET NULL";

            await ExecuteCommandAsync(connection, addJournalIdSql);

            _logger.LogInformation($"Created archive table: {archiveName}");
        }

        private async Task CreateTriggersForTable(MySqlConnection connection, string tableName, List<string> columns)
        {
            var archiveName = $"{tableName}_arch";
            var columnsList = string.Join(", ", columns);
            var valuesList = string.Join(", ", columns.Select(c => $"NEW.{c}"));
            var oldValuesList = string.Join(", ", columns.Select(c => $"OLD.{c}"));

            // Триггер на INSERT
            var insertTriggerSql = $@"
            DROP TRIGGER IF EXISTS {tableName}_insert;
            CREATE TRIGGER {tableName}_insert
            AFTER INSERT ON {tableName}
            FOR EACH ROW
            BEGIN
                DECLARE last_id INT;
            
                INSERT INTO journal (TableName, Operation, OperationDate, UserName)
                VALUES ('{tableName}', 'INSERT', NOW(), USER());
            
                SET last_id = LAST_INSERT_ID();
            
                INSERT INTO {archiveName} ({columnsList}, JournalId)
                VALUES ({valuesList}, last_id);
            END;";

            await ExecuteCommandAsync(connection, insertTriggerSql);

            // Триггер на UPDATE
            var updateTriggerSql = $@"
            DROP TRIGGER IF EXISTS {tableName}_update;
            CREATE TRIGGER {tableName}_update
            AFTER UPDATE ON {tableName}
            FOR EACH ROW
            BEGIN
                DECLARE last_id INT;
            
                INSERT INTO journal (TableName, Operation, OperationDate, UserName)
                VALUES ('{tableName}', 'UPDATE', NOW(), USER());
            
                SET last_id = LAST_INSERT_ID();
            
                INSERT INTO {archiveName} ({columnsList}, JournalId)
                VALUES (NEW.{columnsList.Replace("NEW.", "")}, last_id);
            END;";

            await ExecuteCommandAsync(connection, updateTriggerSql);

            // Триггер на DELETE
            var deleteTriggerSql = $@"
            DROP TRIGGER IF EXISTS {tableName}_delete;
            CREATE TRIGGER {tableName}_delete
            AFTER DELETE ON {tableName}
            FOR EACH ROW
            BEGIN
                DECLARE last_id INT;
            
                INSERT INTO journal (TableName, Operation, OperationDate, UserName)
                VALUES ('{tableName}', 'DELETE', NOW(), USER());
            
                SET last_id = LAST_INSERT_ID();
            
                INSERT INTO {archiveName} ({columnsList}, JournalId)
                VALUES ({oldValuesList}, last_id);
            END;";

            await ExecuteCommandAsync(connection, deleteTriggerSql);

            _logger.LogInformation($"Created triggers for table: {tableName}");
        }

        private async Task CreateTriggersForAllTables(MySqlConnection connection)
        {
            // Получаем список всех таблиц (кроме служебных)
            var tables = new List<string>();
            var getTablesSql = "SHOW TABLES";

            using (var cmd = new MySqlCommand(getTablesSql, connection))
            using (var reader = await cmd.ExecuteReaderAsync())
            {
                while (await reader.ReadAsync())
                {
                    var tableName = reader.GetString(0);
                    if (tableName != "journal" && !tableName.Contains("_arch"))
                    {
                        tables.Add(tableName);
                    }
                }
            }

            // Для каждой таблицы создаем архивную таблицу и триггеры
            foreach (var tableName in tables)
            {
                // Получаем список колонок таблицы
                var columns = await GetTableColumns(connection, tableName);

                // Создаем архивную таблицу
                await CreateArchiveTable(connection, tableName);

                // Создаем триггеры
                await CreateTriggersForTable(connection, tableName, columns);
            }
        }

        private async Task<List<string>> GetTableColumns(MySqlConnection connection, string tableName)
        {
            var columns = new List<string>();
            var sql = $"SHOW COLUMNS FROM {tableName}";

            using (var cmd = new MySqlCommand(sql, connection))
            using (var reader = await cmd.ExecuteReaderAsync())
            {
                while (await reader.ReadAsync())
                {
                    var columnName = reader.GetString(0);
                    // Пропускаем auto_increment колонки при вставке
                    var extra = reader.GetString(5);
                    if (!extra.Contains("auto_increment"))
                    {
                        columns.Add(columnName);
                    }
                }
            }

            return columns;
        }
    }
}
