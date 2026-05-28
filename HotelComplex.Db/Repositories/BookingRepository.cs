using HotelComplex.Db.Models;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HotelComplex.Db.Repositories
{
    public class BookingRepository : IBookingRepository
    {
        readonly DbConnectionFactory _connectionFactory;
        readonly ILogger<BookingRepository> _logger;

        public BookingRepository(DbConnectionFactory connectionFactory, ILogger<BookingRepository> logger)
        {
            _connectionFactory = connectionFactory;
            _logger = logger;
        }

        #region Basic CRUD

        public async Task<Booking?> GetByIdAsync(uint id)
        {
            using var connection = _connectionFactory.CreateConnection();
            await connection.OpenAsync();

            const string sql = @"
                SELECT b.*, 
                       gp.LastName, gp.FirstName, gp.MiddleName, gp.PassportSeries, gp.PassportNumber, gp.Citizenship,
                       r.RoomNumber, r.Floor, r.RoomTypeId, r.Capacity, r.BasePrice, r.Status as RoomStatus,
                       cp.OrganizationName, cp.LegalAddress, cp.ContactPerson, cp.Phone as PartnerPhone, cp.Email as PartnerEmail,
                       c.ConclusionDate, c.ValidUntil, c.DiscountRate
                FROM Booking b
                INNER JOIN GuestProfiles gp ON b.GuestId = gp.UserId
                INNER JOIN Room r ON b.RoomId = r.Id
                LEFT JOIN CorporatePartner cp ON b.PartnerId = cp.Id
                LEFT JOIN Contract c ON b.ContractId = c.Id
                WHERE b.Id = @Id";

            using var cmd = new MySqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@Id", id);

            using var reader = await cmd.ExecuteReaderAsync();
            if (await reader.ReadAsync())
            {
                return MapBookingWithDetailsFromReader((MySqlDataReader)reader);
            }
            return null;
        }

        public async Task<IEnumerable<Booking>> GetAllAsync()
        {
            var bookings = new List<Booking>();

            using var connection = _connectionFactory.CreateConnection();
            await connection.OpenAsync();

            const string sql = "SELECT * FROM Booking ORDER BY CheckInDate DESC";
            using var cmd = new MySqlCommand(sql, connection);
            using var reader = await cmd.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                bookings.Add(MapBookingFromReader((MySqlDataReader)reader));
            }

            return bookings;
        }

        public async Task<IEnumerable<Booking>> GetAllWithDetailsAsync()
        {
            var bookings = new List<Booking>();

            using var connection = _connectionFactory.CreateConnection();
            await connection.OpenAsync();

            const string sql = @"
                SELECT b.*, 
                       gp.LastName, gp.FirstName, gp.MiddleName,
                       r.RoomNumber, r.BasePrice,
                       cp.OrganizationName,
                       c.DiscountRate
                FROM Booking b
                INNER JOIN GuestProfiles gp ON b.GuestId = gp.UserId
                INNER JOIN Room r ON b.RoomId = r.Id
                LEFT JOIN CorporatePartner cp ON b.PartnerId = cp.Id
                LEFT JOIN Contract c ON b.ContractId = c.Id
                ORDER BY b.CheckInDate DESC";

            using var cmd = new MySqlCommand(sql, connection);
            using var reader = await cmd.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                bookings.Add(MapBookingWithDetailsFromReader((MySqlDataReader)reader));
            }

            return bookings;
        }

        public async Task<Booking> CreateAsync(Booking booking)
        {
            using var connection = _connectionFactory.CreateConnection();
            await connection.OpenAsync();
            using var transaction = await connection.BeginTransactionAsync();

            try
            {
                // 1. Проверяем доступность номера
                var isAvailable = await IsRoomAvailableAsync(booking.RoomId, booking.CheckInDate, booking.CheckOutDate, connection, transaction);
                if (!isAvailable)
                {
                    throw new InvalidOperationException("Номер уже забронирован на выбранные даты");
                }

                // 2. Создаем бронирование
                const string insertSql = @"
                    INSERT INTO Booking (GuestId, RoomId, PartnerId, ContractId, CheckInDate, CheckOutDate, Status, Prepayment) 
                    VALUES (@GuestId, @RoomId, @PartnerId, @ContractId, @CheckInDate, @CheckOutDate, @Status, @Prepayment);
                    SELECT LAST_INSERT_ID();";

                uint bookingId;
                using (var cmd = new MySqlCommand(insertSql, connection, transaction))
                {
                    cmd.Parameters.AddWithValue("@GuestId", booking.GuestId);
                    cmd.Parameters.AddWithValue("@RoomId", booking.RoomId);
                    cmd.Parameters.AddWithValue("@PartnerId", booking.PartnerId ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@ContractId", booking.ContractId ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@CheckInDate", booking.CheckInDate);
                    cmd.Parameters.AddWithValue("@CheckOutDate", booking.CheckOutDate);
                    cmd.Parameters.AddWithValue("@Status", "confirmed");
                    cmd.Parameters.AddWithValue("@Prepayment", booking.Prepayment);

                    bookingId = Convert.ToUInt32(await cmd.ExecuteScalarAsync());
                }

                // 3. Обновляем статус номера на "occupied"
                const string updateRoomSql = "UPDATE Room SET Status = 'occupied' WHERE Id = @RoomId";
                using (var cmd = new MySqlCommand(updateRoomSql, connection, transaction))
                {
                    cmd.Parameters.AddWithValue("@RoomId", booking.RoomId);
                    await cmd.ExecuteNonQueryAsync();
                }

                await transaction.CommitAsync();

                booking.Id = bookingId;
                _logger.LogInformation($"Booking created: #{bookingId} for Room #{booking.RoomId}");
                return booking;
            }
            catch (Exception ex)
            {
                await transaction.RollbackAsync();
                _logger.LogError(ex, "Failed to create booking");
                throw;
            }
        }

        public async Task<Booking> UpdateAsync(Booking booking)
        {
            using var connection = _connectionFactory.CreateConnection();
            await connection.OpenAsync();

            const string sql = @"
                UPDATE Booking 
                SET GuestId = @GuestId, 
                    RoomId = @RoomId, 
                    PartnerId = @PartnerId, 
                    ContractId = @ContractId, 
                    CheckInDate = @CheckInDate, 
                    CheckOutDate = @CheckOutDate, 
                    Prepayment = @Prepayment
                WHERE Id = @Id";

            using var cmd = new MySqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@GuestId", booking.GuestId);
            cmd.Parameters.AddWithValue("@RoomId", booking.RoomId);
            cmd.Parameters.AddWithValue("@PartnerId", booking.PartnerId ?? (object)DBNull.Value);
            cmd.Parameters.AddWithValue("@ContractId", booking.ContractId ?? (object)DBNull.Value);
            cmd.Parameters.AddWithValue("@CheckInDate", booking.CheckInDate);
            cmd.Parameters.AddWithValue("@CheckOutDate", booking.CheckOutDate);
            cmd.Parameters.AddWithValue("@Prepayment", booking.Prepayment);
            cmd.Parameters.AddWithValue("@Id", booking.Id);

            await cmd.ExecuteNonQueryAsync();

            _logger.LogInformation($"Booking updated: #{booking.Id}");
            return booking;
        }

        public async Task<bool> DeleteAsync(uint id)
        {
            using var connection = _connectionFactory.CreateConnection();
            await connection.OpenAsync();

            try
            {
                // Получаем RoomId перед удалением
                var roomId = await GetRoomIdByBookingIdAsync(id, connection);

                const string sql = "DELETE FROM Booking WHERE Id = @Id";
                using var cmd = new MySqlCommand(sql, connection);
                cmd.Parameters.AddWithValue("@Id", id);

                var affected = await cmd.ExecuteNonQueryAsync();

                if (affected > 0 && roomId.HasValue)
                {
                    // Возвращаем статус номера на 'free'
                    const string updateRoomSql = "UPDATE Room SET Status = 'free' WHERE Id = @RoomId AND Status = 'occupied'";
                    using var updateCmd = new MySqlCommand(updateRoomSql, connection);
                    updateCmd.Parameters.AddWithValue("@RoomId", roomId.Value);
                    await updateCmd.ExecuteNonQueryAsync();

                    _logger.LogInformation($"Booking deleted: #{id}, Room #{roomId} status reset to free");
                }

                return affected > 0;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error deleting booking {id}");
                throw;
            }
        }

        #endregion

        #region Status Management

        public async Task<bool> ConfirmBookingAsync(uint id)
        {
            using var connection = _connectionFactory.CreateConnection();
            await connection.OpenAsync();

            const string sql = "UPDATE Booking SET Status = 'confirmed' WHERE Id = @Id";
            using var cmd = new MySqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@Id", id);

            var affected = await cmd.ExecuteNonQueryAsync();

            if (affected > 0)
            {
                _logger.LogInformation($"Booking #{id} confirmed");
            }

            return affected > 0;
        }

        public async Task<bool> CancelBookingAsync(uint id)
        {
            using var connection = _connectionFactory.CreateConnection();
            await connection.OpenAsync();
            using var transaction = await connection.BeginTransactionAsync();

            try
            {
                // Получаем RoomId
                var roomId = await GetRoomIdByBookingIdAsync(id, connection, transaction);

                // Обновляем статус бронирования
                const string updateBookingSql = "UPDATE Booking SET Status = 'cancelled' WHERE Id = @Id";
                using var bookingCmd = new MySqlCommand(updateBookingSql, connection, transaction);
                bookingCmd.Parameters.AddWithValue("@Id", id);
                await bookingCmd.ExecuteNonQueryAsync();

                // Освобождаем номер
                if (roomId.HasValue)
                {
                    const string updateRoomSql = "UPDATE Room SET Status = 'free' WHERE Id = @RoomId";
                    using var roomCmd = new MySqlCommand(updateRoomSql, connection, transaction);
                    roomCmd.Parameters.AddWithValue("@RoomId", roomId.Value);
                    await roomCmd.ExecuteNonQueryAsync();
                }

                await transaction.CommitAsync();
                _logger.LogInformation($"Booking #{id} cancelled, Room #{roomId} freed");
                return true;
            }
            catch (Exception ex)
            {
                await transaction.RollbackAsync();
                _logger.LogError(ex, $"Error cancelling booking {id}");
                throw;
            }
        }

        public async Task<bool> CompleteBookingAsync(uint id)
        {
            using var connection = _connectionFactory.CreateConnection();
            await connection.OpenAsync();

            const string sql = "UPDATE Booking SET Status = 'completed' WHERE Id = @Id";
            using var cmd = new MySqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@Id", id);

            var affected = await cmd.ExecuteNonQueryAsync();

            if (affected > 0)
            {
                _logger.LogInformation($"Booking #{id} completed");
            }

            return affected > 0;
        }

        #endregion

        #region Search and Filter

        public async Task<IEnumerable<Booking>> GetBookingsByGuestAsync(uint guestId)
        {
            var bookings = new List<Booking>();

            using var connection = _connectionFactory.CreateConnection();
            await connection.OpenAsync();

            const string sql = @"
                SELECT b.*, r.RoomNumber, r.BasePrice
                FROM Booking b
                INNER JOIN Room r ON b.RoomId = r.Id
                WHERE b.GuestId = @GuestId
                ORDER BY b.CheckInDate DESC";

            using var cmd = new MySqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@GuestId", guestId);

            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                bookings.Add(MapBookingWithRoomFromReader((MySqlDataReader)reader));
            }

            return bookings;
        }

        public async Task<IEnumerable<Booking>> GetBookingsByRoomAsync(ushort roomId)
        {
            var bookings = new List<Booking>();

            using var connection = _connectionFactory.CreateConnection();
            await connection.OpenAsync();

            const string sql = @"
                SELECT b.*, gp.LastName, gp.FirstName
                FROM Booking b
                INNER JOIN GuestProfiles gp ON b.GuestId = gp.UserId
                WHERE b.RoomId = @RoomId
                ORDER BY b.CheckInDate DESC";

            using var cmd = new MySqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@RoomId", roomId);

            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                bookings.Add(MapBookingWithGuestFromReader((MySqlDataReader)reader));
            }

            return bookings;
        }

        public async Task<IEnumerable<Booking>> GetBookingsByStatusAsync(string status)
        {
            var bookings = new List<Booking>();

            using var connection = _connectionFactory.CreateConnection();
            await connection.OpenAsync();

            const string sql = @"
                SELECT b.*, gp.LastName, gp.FirstName, r.RoomNumber
                FROM Booking b
                INNER JOIN GuestProfiles gp ON b.GuestId = gp.UserId
                INNER JOIN Room r ON b.RoomId = r.Id
                WHERE b.Status = @Status
                ORDER BY b.CheckInDate DESC";

            using var cmd = new MySqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@Status", status);

            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                bookings.Add(MapBookingWithDetailsFromReader((MySqlDataReader)reader));
            }

            return bookings;
        }

        public async Task<IEnumerable<Booking>> GetBookingsByDateRangeAsync(DateTime startDate, DateTime endDate)
        {
            var bookings = new List<Booking>();

            using var connection = _connectionFactory.CreateConnection();
            await connection.OpenAsync();

            const string sql = @"
                SELECT b.*, gp.LastName, gp.FirstName, r.RoomNumber
                FROM Booking b
                INNER JOIN GuestProfiles gp ON b.GuestId = gp.UserId
                INNER JOIN Room r ON b.RoomId = r.Id
                WHERE b.CheckInDate >= @StartDate AND b.CheckOutDate <= @EndDate
                ORDER BY b.CheckInDate";

            using var cmd = new MySqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@StartDate", startDate);
            cmd.Parameters.AddWithValue("@EndDate", endDate);

            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                bookings.Add(MapBookingWithDetailsFromReader((MySqlDataReader)reader));
            }

            return bookings;
        }

        public async Task<IEnumerable<Booking>> GetActiveBookingsAsync()
        {
            var bookings = new List<Booking>();

            using var connection = _connectionFactory.CreateConnection();
            await connection.OpenAsync();

            const string sql = @"
                SELECT b.*, gp.LastName, gp.FirstName, r.RoomNumber
                FROM Booking b
                INNER JOIN GuestProfiles gp ON b.GuestId = gp.UserId
                INNER JOIN Room r ON b.RoomId = r.Id
                WHERE b.Status = 'confirmed' 
                  AND b.CheckInDate <= CURDATE() 
                  AND b.CheckOutDate >= CURDATE()
                ORDER BY b.CheckInDate";

            using var cmd = new MySqlCommand(sql, connection);
            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                bookings.Add(MapBookingWithDetailsFromReader((MySqlDataReader)reader));
            }

            return bookings;
        }

        public async Task<IEnumerable<Booking>> GetUpcomingBookingsAsync(int days = 7)
        {
            var bookings = new List<Booking>();

            using var connection = _connectionFactory.CreateConnection();
            await connection.OpenAsync();

            const string sql = @"
                SELECT b.*, gp.LastName, gp.FirstName, r.RoomNumber
                FROM Booking b
                INNER JOIN GuestProfiles gp ON b.GuestId = gp.UserId
                INNER JOIN Room r ON b.RoomId = r.Id
                WHERE b.Status = 'confirmed' 
                  AND b.CheckInDate BETWEEN CURDATE() AND DATE_ADD(CURDATE(), INTERVAL @Days DAY)
                ORDER BY b.CheckInDate";

            using var cmd = new MySqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@Days", days);

            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                bookings.Add(MapBookingWithDetailsFromReader((MySqlDataReader)reader));
            }

            return bookings;
        }

        #endregion

        #region Checks

        public async Task<bool> IsRoomAvailableAsync(ushort roomId, DateTime checkIn, DateTime checkOut, uint? excludeId = null)
        {
            using var connection = _connectionFactory.CreateConnection();
            await connection.OpenAsync();
            return await IsRoomAvailableAsync(roomId, checkIn, checkOut, connection, null, excludeId);
        }

        private async Task<bool> IsRoomAvailableAsync(ushort roomId, DateTime checkIn, DateTime checkOut,
            MySqlConnection connection, MySqlTransaction? transaction = null, uint? excludeId = null)
        {
            // Проверяем статус номера
            const string statusSql = "SELECT Status FROM Room WHERE Id = @RoomId";
            using var statusCmd = new MySqlCommand(statusSql, connection, transaction);
            statusCmd.Parameters.AddWithValue("@RoomId", roomId);

            var status = await statusCmd.ExecuteScalarAsync() as string;
            if (status != "free")
                return false;

            // Проверяем конфликтующие бронирования
            string bookingSql = @"
                SELECT COUNT(*) > 0 FROM Booking 
                WHERE RoomId = @RoomId 
                AND Status = 'confirmed'
                AND ((CheckInDate <= @CheckOut AND CheckOutDate >= @CheckIn))";

            if (excludeId.HasValue)
            {
                bookingSql += " AND Id != @ExcludeId";
            }

            using var bookingCmd = new MySqlCommand(bookingSql, connection, transaction);
            bookingCmd.Parameters.AddWithValue("@RoomId", roomId);
            bookingCmd.Parameters.AddWithValue("@CheckIn", checkIn);
            bookingCmd.Parameters.AddWithValue("@CheckOut", checkOut);
            if (excludeId.HasValue)
            {
                bookingCmd.Parameters.AddWithValue("@ExcludeId", excludeId.Value);
            }

            return !Convert.ToBoolean(await bookingCmd.ExecuteScalarAsync());
        }

        public async Task<bool> HasActiveBookingsAsync(uint guestId)
        {
            using var connection = _connectionFactory.CreateConnection();
            await connection.OpenAsync();

            const string sql = @"
                SELECT COUNT(*) > 0 FROM Booking 
                WHERE GuestId = @GuestId 
                AND Status = 'confirmed' 
                AND CheckOutDate >= CURDATE()";

            using var cmd = new MySqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@GuestId", guestId);

            return Convert.ToBoolean(await cmd.ExecuteScalarAsync());
        }

        #endregion

        #region Statistics

        public async Task<int> GetCountAsync()
        {
            using var connection = _connectionFactory.CreateConnection();
            await connection.OpenAsync();

            const string sql = "SELECT COUNT(*) FROM Booking";
            using var cmd = new MySqlCommand(sql, connection);

            return Convert.ToInt32(await cmd.ExecuteScalarAsync());
        }

        public async Task<int> GetCountByStatusAsync(string status)
        {
            using var connection = _connectionFactory.CreateConnection();
            await connection.OpenAsync();

            const string sql = "SELECT COUNT(*) FROM Booking WHERE Status = @Status";
            using var cmd = new MySqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@Status", status);

            return Convert.ToInt32(await cmd.ExecuteScalarAsync());
        }

        public async Task<decimal> GetTotalRevenueAsync(DateTime? startDate = null, DateTime? endDate = null)
        {
            using var connection = _connectionFactory.CreateConnection();
            await connection.OpenAsync();

            string sql = @"
                SELECT COALESCE(SUM(b.Prepayment + (r.BasePrice * DATEDIFF(b.CheckOutDate, b.CheckInDate) * (1 - COALESCE(c.DiscountRate, 0) / 100))), 0)
                FROM Booking b
                INNER JOIN Room r ON b.RoomId = r.Id
                LEFT JOIN Contract c ON b.ContractId = c.Id
                WHERE b.Status = 'completed'";

            if (startDate.HasValue)
            {
                sql += " AND b.CheckOutDate >= @StartDate";
            }
            if (endDate.HasValue)
            {
                sql += " AND b.CheckOutDate <= @EndDate";
            }

            using var cmd = new MySqlCommand(sql, connection);
            if (startDate.HasValue)
            {
                cmd.Parameters.AddWithValue("@StartDate", startDate.Value);
            }
            if (endDate.HasValue)
            {
                cmd.Parameters.AddWithValue("@EndDate", endDate.Value);
            }

            var result = await cmd.ExecuteScalarAsync();
            return result != DBNull.Value ? Convert.ToDecimal(result) : 0;
        }

        public async Task<Dictionary<string, int>> GetStatusStatisticsAsync()
        {
            var stats = new Dictionary<string, int>();

            using var connection = _connectionFactory.CreateConnection();
            await connection.OpenAsync();

            const string sql = @"
                SELECT Status, COUNT(*) as Count 
                FROM Booking 
                GROUP BY Status";

            using var cmd = new MySqlCommand(sql, connection);
            using var reader = (MySqlDataReader)await cmd.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                var status = reader.GetString("Status");
                var count = reader.GetInt32("Count");
                stats.Add(status, count);
            }

            return stats;
        }

        #endregion

        #region Calculations

        public async Task<decimal> CalculateTotalPriceAsync(ushort roomId, DateTime checkIn, DateTime checkOut)
        {
            using var connection = _connectionFactory.CreateConnection();
            await connection.OpenAsync();

            var nights = (int)(checkOut - checkIn).TotalDays;

            const string sql = "SELECT BasePrice FROM Room WHERE Id = @RoomId";
            using var cmd = new MySqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@RoomId", roomId);

            var basePrice = Convert.ToDecimal(await cmd.ExecuteScalarAsync());
            return basePrice * nights;
        }

        #endregion

        #region Helper Methods

        private async Task<ushort?> GetRoomIdByBookingIdAsync(uint bookingId, MySqlConnection connection, MySqlTransaction? transaction = null)
        {
            const string sql = "SELECT RoomId FROM Booking WHERE Id = @Id";
            using var cmd = new MySqlCommand(sql, connection, transaction);
            cmd.Parameters.AddWithValue("@Id", bookingId);

            var result = await cmd.ExecuteScalarAsync();
            return result != DBNull.Value ? Convert.ToUInt16(result) : null;
        }

        #endregion

        #region Mapping Methods

        private Booking MapBookingFromReader(MySqlDataReader reader)
        {
            return new Booking
            {
                Id = reader.GetUInt32("Id"),
                GuestId = reader.GetUInt32("GuestId"),
                RoomId = reader.GetUInt16("RoomId"),
                PartnerId = reader.IsDBNull(reader.GetOrdinal("PartnerId"))
                    ? null : reader.GetUInt32("PartnerId"),
                ContractId = reader.IsDBNull(reader.GetOrdinal("ContractId"))
                    ? null : reader.GetUInt32("ContractId"),
                CheckInDate = reader.GetDateTime("CheckInDate"),
                CheckOutDate = reader.GetDateTime("CheckOutDate"),
                Status = reader.GetString("Status"),
                Prepayment = reader.GetDecimal("Prepayment")
            };
        }

        private Booking MapBookingWithDetailsFromReader(MySqlDataReader reader)
        {
            var booking = MapBookingFromReader(reader);

            // Маппинг гостя
            booking.Guest = new GuestProfile
            {
                UserId = booking.GuestId,
                LastName = reader.GetString("LastName"),
                FirstName = reader.GetString("FirstName"),
                MiddleName = reader.IsDBNull(reader.GetOrdinal("MiddleName"))
                    ? null : reader.GetString("MiddleName"),
                PassportSeries = reader.IsDBNull(reader.GetOrdinal("PassportSeries"))
                    ? string.Empty : reader.GetString("PassportSeries"),
                PassportNumber = reader.IsDBNull(reader.GetOrdinal("PassportNumber"))
                    ? string.Empty : reader.GetString("PassportNumber"),
                Citizenship = reader.IsDBNull(reader.GetOrdinal("Citizenship"))
                    ? "РФ" : reader.GetString("Citizenship")
            };

            // Маппинг комнаты
            booking.Room = new Room
            {
                Id = booking.RoomId,
                RoomNumber = reader.GetUInt16("RoomNumber"),
                Floor = reader.IsDBNull(reader.GetOrdinal("Floor"))
                    ? (byte)0 : reader.GetByte("Floor"),
                BasePrice = reader.IsDBNull(reader.GetOrdinal("BasePrice"))
                    ? 0 : reader.GetDecimal("BasePrice")
            };

            // Маппинг партнера
            if (!reader.IsDBNull(reader.GetOrdinal("OrganizationName")))
            {
                booking.Partner = new CorporatePartner
                {
                    Id = booking.PartnerId ?? 0,
                    OrganizationName = reader.GetString("OrganizationName"),
                    LegalAddress = reader.IsDBNull(reader.GetOrdinal("LegalAddress"))
                        ? string.Empty : reader.GetString("LegalAddress"),
                    ContactPerson = reader.IsDBNull(reader.GetOrdinal("ContactPerson"))
                        ? string.Empty : reader.GetString("ContactPerson"),
                    Phone = reader.IsDBNull(reader.GetOrdinal("PartnerPhone"))
                        ? string.Empty : reader.GetString("PartnerPhone"),
                    Email = reader.IsDBNull(reader.GetOrdinal("PartnerEmail"))
                        ? string.Empty : reader.GetString("PartnerEmail")
                };
            }

            // Маппинг договора
            if (!reader.IsDBNull(reader.GetOrdinal("ConclusionDate")))
            {
                booking.Contract = new Contract
                {
                    Id = booking.ContractId ?? 0,
                    PartnerId = booking.PartnerId ?? 0,
                    ConclusionDate = reader.GetDateTime("ConclusionDate"),
                    ValidUntil = reader.GetDateTime("ValidUntil"),
                    DiscountRate = reader.GetDecimal("DiscountRate")
                };
            }

            return booking;
        }

        private Booking MapBookingWithRoomFromReader(MySqlDataReader reader)
        {
            var booking = MapBookingFromReader(reader);

            booking.Room = new Room
            {
                Id = booking.RoomId,
                RoomNumber = reader.GetUInt16("RoomNumber"),
                BasePrice = reader.GetDecimal("BasePrice")
            };

            return booking;
        }

        private Booking MapBookingWithGuestFromReader(MySqlDataReader reader)
        {
            var booking = MapBookingFromReader(reader);

            booking.Guest = new GuestProfile
            {
                UserId = booking.GuestId,
                LastName = reader.GetString("LastName"),
                FirstName = reader.GetString("FirstName")
            };

            return booking;
        }

        #endregion
    }
}
