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

            // Создаем все таблицы последовательно
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
    }
}
