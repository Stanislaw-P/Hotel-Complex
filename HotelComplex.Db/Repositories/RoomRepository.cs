using HotelComplex.Db.Models;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using System.Data;

namespace HotelComplex.Db.Repositories
{
    public class RoomRepository : IRoomRepository
    {
        readonly DbConnectionFactory _connectionFactory;
        readonly ILogger<RoomRepository> _logger;

        public RoomRepository(DbConnectionFactory connectionFactory, ILogger<RoomRepository> logger)
        {
            _connectionFactory = connectionFactory;
            _logger = logger;
        }

        public async Task<Room> CreateAsync(Room room)
        {
            using var connection = _connectionFactory.CreateConnection();
            await connection.OpenAsync();

            const string sql = @"
                INSERT INTO Room (RoomNumber, Floor, RoomTypeId, Capacity, BasePrice, Status) 
                VALUES (@RoomNumber, @Floor, @RoomTypeId, @Capacity, @BasePrice, @Status);
                SELECT LAST_INSERT_ID();";

            using var cmd = new MySqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@RoomNumber", room.RoomNumber);
            cmd.Parameters.AddWithValue("@Floor", room.Floor);
            cmd.Parameters.AddWithValue("@RoomTypeId", room.RoomTypeId);
            cmd.Parameters.AddWithValue("@Capacity", room.Capacity);
            cmd.Parameters.AddWithValue("@BasePrice", room.BasePrice);
            cmd.Parameters.AddWithValue("@Status", room.Status);

            var id = Convert.ToUInt16(await cmd.ExecuteScalarAsync());
            room.Id = id;

            _logger.LogInformation($"Room created: №{room.RoomNumber}");
            return room;
        }

        public async Task<bool> DeleteAsync(ushort id)
        {
            using var connection = _connectionFactory.CreateConnection();
            await connection.OpenAsync();

            try
            {
                // Проверяем наличие активных бронирований
                const string checkSql = "SELECT COUNT(*) > 0 FROM Booking WHERE RoomId = @Id AND Status = 'confirmed'";
                using var checkCmd = new MySqlCommand(checkSql, connection);
                checkCmd.Parameters.AddWithValue("@Id", id);

                var hasActiveBookings = Convert.ToBoolean(await checkCmd.ExecuteScalarAsync());

                if (hasActiveBookings)
                {
                    throw new InvalidOperationException("Невозможно удалить номер с активными бронированиями");
                }

                const string deleteSql = "DELETE FROM Room WHERE Id = @Id";
                using var deleteCmd = new MySqlCommand(deleteSql, connection);
                deleteCmd.Parameters.AddWithValue("@Id", id);

                var affected = await deleteCmd.ExecuteNonQueryAsync();

                if (affected > 0)
                {
                    _logger.LogInformation($"Room deleted: Id = {id}");
                }

                return affected > 0;
            }
            catch (MySqlException ex)
            {
                _logger.LogError(ex, $"Error deleting room {id}");
                throw;
            }
        }

        public async Task<IEnumerable<Room>> GetAllAsync()
        {
            var rooms = new List<Room>();

            using var connection = _connectionFactory.CreateConnection();
            await connection.OpenAsync();

            const string sql = "SELECT * FROM Room ORDER BY RoomNumber";
            using var cmd = new MySqlCommand(sql, connection);
            using var reader = await cmd.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                rooms.Add(MapRoomFromReader((MySqlDataReader)reader));
            }

            return rooms;
        }

        public async Task<IEnumerable<Room>> GetAllWithTypesAsync()
        {
            var rooms = new List<Room>();

            using var connection = _connectionFactory.CreateConnection();
            await connection.OpenAsync();

            const string sql = @"
                SELECT r.*, rt.Id as RoomTypeId, rt.Name as RoomTypeName, rt.Description as RoomTypeDescription
                FROM Room r
                LEFT JOIN RoomType rt ON r.RoomTypeId = rt.Id
                ORDER BY r.RoomNumber";

            using var cmd = new MySqlCommand(sql, connection);
            using var reader = await cmd.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                rooms.Add(MapRoomWithTypeFromReader((MySqlDataReader)reader));
            }

            return rooms;
        }

        public async Task<Room?> GetByIdAsync(ushort id)
        {
            using var connection = _connectionFactory.CreateConnection();
            await connection.OpenAsync();

            const string sql = @"
                SELECT r.*, rt.Id as RoomTypeId, rt.Name as RoomTypeName, rt.Description as RoomTypeDescription
                FROM Room r
                LEFT JOIN RoomType rt ON r.RoomTypeId = rt.Id
                WHERE r.Id = @Id";

            using var cmd = new MySqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@Id", id);

            using var reader = await cmd.ExecuteReaderAsync();
            if (await reader.ReadAsync())
            {
                return MapRoomWithTypeFromReader((MySqlDataReader)reader);
            }
            return null;
        }

        public async Task<Room?> GetByRoomNumberAsync(ushort roomNumber)
        {
            using var connection = _connectionFactory.CreateConnection();
            await connection.OpenAsync();

            const string sql = @"
                SELECT r.*, rt.Id as RoomTypeId, rt.Name as RoomTypeName, rt.Description as RoomTypeDescription
                FROM Room r
                LEFT JOIN RoomType rt ON r.RoomTypeId = rt.Id
                WHERE r.RoomNumber = @RoomNumber";

            using var cmd = new MySqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@RoomNumber", roomNumber);

            using var reader = await cmd.ExecuteReaderAsync();
            if (await reader.ReadAsync())
            {
                return MapRoomWithTypeFromReader((MySqlDataReader)reader);
            }
            return null;
        }

        public async Task<Room> UpdateAsync(Room room)
        {
            using var connection = _connectionFactory.CreateConnection();
            await connection.OpenAsync();

            const string sql = @"
                UPDATE Room 
                SET RoomNumber = @RoomNumber, 
                    Floor = @Floor, 
                    RoomTypeId = @RoomTypeId, 
                    Capacity = @Capacity, 
                    BasePrice = @BasePrice,
                    Status = @Status
                WHERE Id = @Id";

            using var cmd = new MySqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@RoomNumber", room.RoomNumber);
            cmd.Parameters.AddWithValue("@Floor", room.Floor);
            cmd.Parameters.AddWithValue("@RoomTypeId", room.RoomTypeId);
            cmd.Parameters.AddWithValue("@Capacity", room.Capacity);
            cmd.Parameters.AddWithValue("@BasePrice", room.BasePrice);
            cmd.Parameters.AddWithValue("@Status", room.Status);
            cmd.Parameters.AddWithValue("@Id", room.Id);

            await cmd.ExecuteNonQueryAsync();

            _logger.LogInformation($"Room updated: №{room.RoomNumber}");
            return room;
        }

        // Получение доступных комнат
        public async Task<IEnumerable<Room>> GetAvailableRoomsAsync()
        {
            var rooms = new List<Room>();

            using var connection = _connectionFactory.CreateConnection();
            await connection.OpenAsync();

            const string sql = @"
                SELECT r.*, rt.Id as RoomTypeId, rt.Name as RoomTypeName, rt.Description as RoomTypeDescription
                FROM Room r
                LEFT JOIN RoomType rt ON r.RoomTypeId = rt.Id
                WHERE r.Status = 'free'
                ORDER BY r.RoomNumber";

            using var cmd = new MySqlCommand(sql, connection);
            using var reader = await cmd.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                rooms.Add(MapRoomWithTypeFromReader((MySqlDataReader)reader));
            }

            return rooms;
        }

        // Получение комнат по статусу
        public async Task<IEnumerable<Room>> GetRoomsByStatusAsync(string status)
        {
            var rooms = new List<Room>();

            using var connection = _connectionFactory.CreateConnection();
            await connection.OpenAsync();

            const string sql = @"
                SELECT r.*, rt.Id as RoomTypeId, rt.Name as RoomTypeName, rt.Description as RoomTypeDescription
                FROM Room r
                LEFT JOIN RoomType rt ON r.RoomTypeId = rt.Id
                WHERE r.Status = @Status
                ORDER BY r.RoomNumber";

            using var cmd = new MySqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@Status", status);
            using var reader = await cmd.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                rooms.Add(MapRoomWithTypeFromReader((MySqlDataReader)reader));
            }

            return rooms;
        }

        // Получение комнат по этажу
        public async Task<IEnumerable<Room>> GetRoomsByFloorAsync(byte floor)
        {
            var rooms = new List<Room>();

            using var connection = _connectionFactory.CreateConnection();
            await connection.OpenAsync();

            const string sql = @"
                SELECT r.*, rt.Id as RoomTypeId, rt.Name as RoomTypeName, rt.Description as RoomTypeDescription
                FROM Room r
                LEFT JOIN RoomType rt ON r.RoomTypeId = rt.Id
                WHERE r.Floor = @Floor
                ORDER BY r.RoomNumber";

            using var cmd = new MySqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@Floor", floor);
            using var reader = await cmd.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                rooms.Add(MapRoomWithTypeFromReader((MySqlDataReader)reader));
            }

            return rooms;
        }

        // Получение комнат по типу
        public async Task<IEnumerable<Room>> GetRoomsByTypeAsync(ushort roomTypeId)
        {
            var rooms = new List<Room>();

            using var connection = _connectionFactory.CreateConnection();
            await connection.OpenAsync();

            const string sql = @"
                SELECT r.*, rt.Id as RoomTypeId, rt.Name as RoomTypeName, rt.Description as RoomTypeDescription
                FROM Room r
                LEFT JOIN RoomType rt ON r.RoomTypeId = rt.Id
                WHERE r.RoomTypeId = @RoomTypeId
                ORDER BY r.RoomNumber";

            using var cmd = new MySqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@RoomTypeId", roomTypeId);
            using var reader = await cmd.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                rooms.Add(MapRoomWithTypeFromReader((MySqlDataReader)reader));
            }

            return rooms;
        }

        // Проверка существования номера
        public async Task<bool> RoomNumberExistsAsync(ushort roomNumber, ushort? excludeId = null)
        {
            using var connection = _connectionFactory.CreateConnection();
            await connection.OpenAsync();

            string sql;
            if (excludeId.HasValue)
            {
                sql = "SELECT COUNT(*) > 0 FROM Room WHERE RoomNumber = @RoomNumber AND Id != @ExcludeId";
            }
            else
            {
                sql = "SELECT COUNT(*) > 0 FROM Room WHERE RoomNumber = @RoomNumber";
            }

            using var cmd = new MySqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@RoomNumber", roomNumber);
            if (excludeId.HasValue)
            {
                cmd.Parameters.AddWithValue("@ExcludeId", excludeId.Value);
            }

            return Convert.ToBoolean(await cmd.ExecuteScalarAsync());
        }

        // Проверка доступности номера на даты
        public async Task<bool> IsRoomAvailableAsync(ushort roomId, DateTime checkIn, DateTime checkOut)
        {
            using var connection = _connectionFactory.CreateConnection();
            await connection.OpenAsync();

            // Проверяем статус номера
            const string statusSql = "SELECT Status FROM Room WHERE Id = @RoomId";
            using var statusCmd = new MySqlCommand(statusSql, connection);
            statusCmd.Parameters.AddWithValue("@RoomId", roomId);

            var status = await statusCmd.ExecuteScalarAsync() as string;
            if (status != "free")
                return false;

            // Проверяем конфликтующие бронирования
            const string bookingSql = @"
                SELECT COUNT(*) > 0 FROM Booking 
                WHERE RoomId = @RoomId 
                AND Status = 'confirmed'
                AND ((CheckInDate <= @CheckOut AND CheckOutDate >= @CheckIn))";

            using var bookingCmd = new MySqlCommand(bookingSql, connection);
            bookingCmd.Parameters.AddWithValue("@RoomId", roomId);
            bookingCmd.Parameters.AddWithValue("@CheckIn", checkIn);
            bookingCmd.Parameters.AddWithValue("@CheckOut", checkOut);

            return !Convert.ToBoolean(await bookingCmd.ExecuteScalarAsync());
        }

        // Изменение статуса комнаты
        public async Task<bool> ChangeStatusAsync(ushort roomId, string status)
        {
            using var connection = _connectionFactory.CreateConnection();
            await connection.OpenAsync();

            var validStatuses = new[] { "free", "occupied", "cleaning", "repair" };
            if (!validStatuses.Contains(status))
                throw new ArgumentException($"Invalid status: {status}");

            const string sql = "UPDATE Room SET Status = @Status WHERE Id = @Id";
            using var cmd = new MySqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@Status", status);
            cmd.Parameters.AddWithValue("@Id", roomId);

            var affected = await cmd.ExecuteNonQueryAsync();

            if (affected > 0)
            {
                _logger.LogInformation($"Room {roomId} status changed to {status}");
            }

            return affected > 0;
        }

        // Статистика
        public async Task<int> GetCountAsync()
        {
            using var connection = _connectionFactory.CreateConnection();
            await connection.OpenAsync();

            const string sql = "SELECT COUNT(*) FROM Room";
            using var cmd = new MySqlCommand(sql, connection);

            return Convert.ToInt32(await cmd.ExecuteScalarAsync());
        }

        public async Task<int> GetAvailableCountAsync()
        {
            using var connection = _connectionFactory.CreateConnection();
            await connection.OpenAsync();

            const string sql = "SELECT COUNT(*) FROM Room WHERE Status = 'free'";
            using var cmd = new MySqlCommand(sql, connection);

            return Convert.ToInt32(await cmd.ExecuteScalarAsync());
        }

        public async Task<Dictionary<string, int>> GetStatusStatisticsAsync()
        {
            var stats = new Dictionary<string, int>();

            using var connection = _connectionFactory.CreateConnection();
            await connection.OpenAsync();

            const string sql = @"
                SELECT Status, COUNT(*) as Count 
                FROM Room 
                GROUP BY Status";

            using var cmd = new MySqlCommand(sql, connection);
            using var reader = await cmd.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                var status = reader.GetString("Status");
                var count = reader.GetInt32("Count");
                stats.Add(status, count);
            }

            return stats;
        }


        private Room MapRoomFromReader(MySqlDataReader reader)
        {
            return new Room
            {
                Id = reader.GetUInt16("Id"),
                RoomNumber = reader.GetUInt16("RoomNumber"),
                Floor = reader.GetByte("Floor"),
                RoomTypeId = reader.GetUInt16("RoomTypeId"),
                Capacity = reader.GetByte("Capacity"),
                BasePrice = reader.GetDecimal("BasePrice"),
                Status = reader.GetString("Status")
            };
        }

        private Room MapRoomWithTypeFromReader(MySqlDataReader reader)
        {
            var room = MapRoomFromReader(reader);

            if (!reader.IsDBNull(reader.GetOrdinal("RoomTypeName")))
            {
                room.RoomType = new RoomType
                {
                    Id = reader.GetUInt16("RoomTypeId"),
                    Name = reader.GetString("RoomTypeName"),
                    Description = reader.IsDBNull(reader.GetOrdinal("RoomTypeDescription"))
                        ? null : reader.GetString("RoomTypeDescription")
                };
            }

            return room;
        }
    }
}
