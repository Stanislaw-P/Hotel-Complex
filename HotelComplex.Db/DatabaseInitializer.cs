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
                await TestConnectionAsync();
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

            await ExecuteCommandAsync(connection, "CREATE DATABASE IF NOT EXISTS HotelComplex;");
            await connection.ChangeDatabaseAsync("HotelComplex");

            await CreateRolesTable(connection);
            await CreateUsersTable(connection);
            await CreateGuestProfilesTable(connection);
            await CreateEmployeeProfilesTable(connection);

            await CreateJournalTable(connection);
            await CreateRoomTypeTable(connection);
            await CreateRoomTable(connection);
            await CreateCorporatePartnerTable(connection);
            await CreateContractTable(connection);
            await CreateServiceTable(connection);
            await CreateBookingTable(connection);
            await CreateStayTable(connection);
            await CreateServiceOrderTable(connection);
            await CreateInvoiceTable(connection);
            await CreateReviewTable(connection);

            await InsertInitialData(connection);
            await DropArchiveTables(connection);
            await CreateTriggersForAllTables(connection);

            _logger.LogInformation("All tables created successfully");
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
                if (ex.Number != 1061 && ex.Number != 1050 && ex.Number != 1062)
                {
                    _logger.LogWarning(ex, "Error executing SQL command: {Command}", commandText);
                }
            }
        }

        #region Tables Creation

        private async Task CreateRolesTable(MySqlConnection connection)
        {
            var sql = @"
                CREATE TABLE IF NOT EXISTS Roles (
                    Id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                    Name VARCHAR(50) NOT NULL UNIQUE,
                    Description VARCHAR(200)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;";

            await ExecuteCommandAsync(connection, sql);
            _logger.LogInformation("Roles table created");
        }

        private async Task CreateUsersTable(MySqlConnection connection)
        {
            var sql = @"
                CREATE TABLE IF NOT EXISTS Users (
                    Id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                    Email VARCHAR(100) NOT NULL UNIQUE,
                    PasswordHash VARCHAR(255) NOT NULL,
                    Phone VARCHAR(20) NOT NULL,
                    RoleId INT UNSIGNED NOT NULL,
                    IsActive BOOLEAN DEFAULT TRUE,
                    CreatedAt DATETIME NOT NULL,
                    LastLoginAt DATETIME NULL,
                    INDEX idx_Email (Email),
                    INDEX idx_RoleId (RoleId),
                    FOREIGN KEY (RoleId) REFERENCES Roles(Id) ON DELETE RESTRICT
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;";

            await ExecuteCommandAsync(connection, sql);
            _logger.LogInformation("Users table created");
        }

        private async Task CreateGuestProfilesTable(MySqlConnection connection)
        {
            var sql = @"
                CREATE TABLE IF NOT EXISTS GuestProfiles (
                    UserId INT UNSIGNED PRIMARY KEY,
                    LastName VARCHAR(50) NOT NULL,
                    FirstName VARCHAR(50) NOT NULL,
                    MiddleName VARCHAR(50),
                    PassportSeries VARCHAR(10) NOT NULL,
                    PassportNumber VARCHAR(20) NOT NULL,
                    Citizenship VARCHAR(50) DEFAULT 'РФ',
                    UNIQUE KEY Unique_Passport (PassportSeries, PassportNumber),
                    INDEX idx_Name (LastName, FirstName),
                    FOREIGN KEY (UserId) REFERENCES Users(Id) ON DELETE CASCADE
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;";

            await ExecuteCommandAsync(connection, sql);
            _logger.LogInformation("GuestProfiles table created");
        }

        private async Task CreateEmployeeProfilesTable(MySqlConnection connection)
        {
            var sql = @"
                CREATE TABLE IF NOT EXISTS EmployeeProfiles (
                    UserId INT UNSIGNED PRIMARY KEY,
                    LastName VARCHAR(50) NOT NULL,
                    FirstName VARCHAR(50) NOT NULL,
                    MiddleName VARCHAR(50),
                    Position VARCHAR(50) NOT NULL,
                    HireDate DATE NOT NULL,
                    Salary DECIMAL(10,2),
                    INDEX idx_Name (LastName, FirstName),
                    FOREIGN KEY (UserId) REFERENCES Users(Id) ON DELETE CASCADE
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;";

            await ExecuteCommandAsync(connection, sql);
            _logger.LogInformation("EmployeeProfiles table created");
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
                    FOREIGN KEY (GuestId) REFERENCES GuestProfiles(UserId) ON DELETE RESTRICT,
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
                    FOREIGN KEY (GuestId) REFERENCES GuestProfiles(UserId) ON DELETE RESTRICT,
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
                    FOREIGN KEY (GuestId) REFERENCES GuestProfiles(UserId) ON DELETE RESTRICT,
                    FOREIGN KEY (EmployeeId) REFERENCES EmployeeProfiles(UserId) ON DELETE RESTRICT
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
                    FOREIGN KEY (GuestId) REFERENCES GuestProfiles(UserId) ON DELETE RESTRICT,
                    FOREIGN KEY (StayId) REFERENCES Stay(Id) ON DELETE RESTRICT
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;";

            await ExecuteCommandAsync(connection, sql);
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

        #endregion

        #region Initial Data

        private async Task InsertInitialData(MySqlConnection connection)
        {
            await InsertRoles(connection);
            await InsertAdminUser(connection);
            await InsertRoomTypes(connection);
            await InsertServices(connection);
            await InsertCorporatePartners(connection);  // ← добавьте этот метод
            await InsertContracts(connection);
            await GenerateRooms(connection);
        }

        private async Task InsertRoles(MySqlConnection connection)
        {
            var sql = @"
                INSERT IGNORE INTO Roles (Name, Description) VALUES
                ('Admin', 'Полный доступ ко всем функциям системы'),
                ('Manager', 'Управление бронированиями и гостями'),
                ('User', 'Обычный пользователь');";

            await ExecuteCommandAsync(connection, sql);
            _logger.LogInformation("Roles inserted");
        }

        private async Task InsertAdminUser(MySqlConnection connection)
        {
            // Получаем ID роли Admin
            var getRoleIdSql = "SELECT Id FROM Roles WHERE Name = 'Admin'";
            uint roleId;
            using (var cmd = new MySqlCommand(getRoleIdSql, connection))
            {
                roleId = Convert.ToUInt32(await cmd.ExecuteScalarAsync());
            }

            // Проверяем, существует ли уже admin
            var checkSql = "SELECT COUNT(*) FROM Users WHERE Email = 'admin@hotel.com'";
            using var checkCmd = new MySqlCommand(checkSql, connection);
            var exists = Convert.ToInt32(await checkCmd.ExecuteScalarAsync());

            if (exists == 0)
            {
                // Создаем пользователя
                var insertUserSql = @"
                    INSERT INTO Users (Email, PasswordHash, Phone, RoleId, IsActive, CreatedAt)
                    VALUES ('admin@hotel.com', 'admin123', '+7 (999) 123-45-67', @RoleId, TRUE, NOW());
                    SELECT LAST_INSERT_ID();";

                uint userId;
                using (var cmd = new MySqlCommand(insertUserSql, connection))
                {
                    cmd.Parameters.AddWithValue("@RoleId", roleId);
                    userId = Convert.ToUInt32(await cmd.ExecuteScalarAsync());
                }

                // Создаем профиль сотрудника
                var insertProfileSql = @"
                    INSERT INTO EmployeeProfiles (UserId, LastName, FirstName, MiddleName, Position, HireDate, Salary)
                    VALUES (@UserId, 'Admin', 'System', NULL, 'Главный администратор', CURDATE(), 50000);";

                using var profileCmd = new MySqlCommand(insertProfileSql, connection);
                profileCmd.Parameters.AddWithValue("@UserId", userId);
                await profileCmd.ExecuteNonQueryAsync();

                _logger.LogInformation("Default admin user created: admin@hotel.com / admin123");
            }
        }

        private async Task InsertRoomTypes(MySqlConnection connection)
        {
            var sql = @"
                INSERT IGNORE INTO RoomType (Name, Description) VALUES
                ('Стандарт', 'Стандартный номер с одной кроватью'),
                ('Полулюкс', 'Улучшенный номер с дополнительным пространством'),
                ('Люкс', 'Номер повышенной комфортности'),
                ('Апартаменты', 'Просторные апартаменты с гостиной зоной');";

            await ExecuteCommandAsync(connection, sql);
        }

        private async Task InsertServices(MySqlConnection connection)
        {
            var sql = @"
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

            await ExecuteCommandAsync(connection, sql);
        }

        private async Task InsertCorporatePartners(MySqlConnection connection)
        {
            var sql = @"
        INSERT IGNORE INTO CorporatePartner (OrganizationName, LegalAddress, ContactPerson, Phone, Email) VALUES
        ('ООО ''Рога и Копыта''', 'г. Москва, ул. Ленина, д. 1', 'Иванов И.И.', '+7 (999) 111-22-33', 'partner1@mail.ru'),
        ('ЗАО ''ТехноПром''', 'г. Санкт-Петербург, пр. Невский, д. 100', 'Петров П.П.', '+7 (999) 222-33-44', 'partner2@mail.ru'),
        ('АО ''БизнесТревел''', 'г. Новосибирск, ул. Советская, д. 50', 'Сидоров С.С.', '+7 (999) 333-44-55', 'partner3@mail.ru');";

            await ExecuteCommandAsync(connection, sql);
            _logger.LogInformation("Corporate partners inserted");
        }

        private async Task InsertContracts(MySqlConnection connection)
        {
            // Получаем ID партнеров
            var getPartnerIds = "SELECT Id, OrganizationName FROM CorporatePartner";
            var partners = new List<(uint Id, string Name)>();

            using (var command = new MySqlCommand(getPartnerIds, connection))
            using (var reader = (MySqlDataReader)await command.ExecuteReaderAsync())
            {
                while (await reader.ReadAsync())
                {
                    partners.Add((reader.GetUInt32("Id"), reader.GetString("OrganizationName")));
                }
            }

            if (partners.Count == 0) return;

            // Определяем ID для вставки (если партнеров меньше 3, используем первого)
            uint partnerId1 = partners.Count > 0 ? partners[0].Id : 1;
            uint partnerId2 = partners.Count > 1 ? partners[1].Id : partnerId1;
            uint partnerId3 = partners.Count > 2 ? partners[2].Id : partnerId1;

            var sql = @"
                INSERT IGNORE INTO Contract (PartnerId, ConclusionDate, ValidUntil, DiscountRate) VALUES
                (@PartnerId1, '2024-01-15', '2024-12-31', 5.00),
                (@PartnerId2, '2024-02-10', '2024-12-31', 10.00),
                (@PartnerId3, '2024-03-20', '2025-03-19', 15.00);";

            using (var cmd = new MySqlCommand(sql, connection))
            {
                cmd.Parameters.AddWithValue("@PartnerId1", partnerId1);
                cmd.Parameters.AddWithValue("@PartnerId2", partnerId2);
                cmd.Parameters.AddWithValue("@PartnerId3", partnerId3);
                await cmd.ExecuteNonQueryAsync();
            }

            _logger.LogInformation("Contracts inserted");
        }

        private async Task GenerateRooms(MySqlConnection connection)
        {
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

        #endregion

        #region Archive and Triggers

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

            var createArchiveSql = $"CREATE TABLE IF NOT EXISTS {archiveName} LIKE {tableName}";
            await ExecuteCommandAsync(connection, createArchiveSql);

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
                    VALUES ({valuesList}, last_id);
                END;";

            await ExecuteCommandAsync(connection, updateTriggerSql);

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
            var tables = new List<string>();
            var getTablesSql = "SHOW TABLES";

            using (var cmd = new MySqlCommand(getTablesSql, connection))
            using (var reader = await cmd.ExecuteReaderAsync())
            {
                while (await reader.ReadAsync())
                {
                    var tableName = reader.GetString(0);
                    if (tableName != "journal" && !tableName.Contains("_arch") && tableName != "Roles")
                    {
                        tables.Add(tableName);
                    }
                }
            }

            foreach (var tableName in tables)
            {
                var columns = await GetTableColumns(connection, tableName);
                await CreateArchiveTable(connection, tableName);
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
                    var extra = reader.GetString(5);
                    if (!extra.Contains("auto_increment"))
                    {
                        columns.Add(columnName);
                    }
                }
            }

            return columns;
        }

        #endregion
    }
}
