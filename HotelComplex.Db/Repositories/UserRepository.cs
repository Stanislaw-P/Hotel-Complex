using HotelComplex.Db.Models;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HotelComplex.Db.Repositories
{
    public class UserRepository : IUserRepository
    {
        readonly DbConnectionFactory _connectionFactory;
        readonly ILogger<UserRepository> _logger;

        public UserRepository(DbConnectionFactory connectionFactory, ILogger<UserRepository> logger)
        {
            _connectionFactory = connectionFactory;
            _logger = logger;
        }

        #region Basic CRUD

        public async Task<User?> GetByIdAsync(uint id)
        {
            using var connection = _connectionFactory.CreateConnection();
            await connection.OpenAsync();

            const string sql = @"
                SELECT u.*, r.Id as RoleId, r.Name as RoleName, r.Description as RoleDescription
                FROM Users u
                INNER JOIN Roles r ON u.RoleId = r.Id
                WHERE u.Id = @Id";

            using var cmd = new MySqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@Id", id);

            using var reader = await cmd.ExecuteReaderAsync();
            if (await reader.ReadAsync())
            {
                var user = MapUserFromReader((MySqlDataReader)reader);
                user.Role = MapRoleFromReader((MySqlDataReader)reader, "RoleId", "RoleName", "RoleDescription");
                return user;
            }
            return null;
        }

        public async Task<User?> GetByEmailAsync(string email)
        {
            using var connection = _connectionFactory.CreateConnection();
            await connection.OpenAsync();

            const string sql = @"
                SELECT u.*, r.Id as RoleId, r.Name as RoleName, r.Description as RoleDescription
                FROM Users u
                INNER JOIN Roles r ON u.RoleId = r.Id
                WHERE u.Email = @Email";

            using var cmd = new MySqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@Email", email);

            using var reader = await cmd.ExecuteReaderAsync();
            if (await reader.ReadAsync())
            {
                var user = MapUserFromReader((MySqlDataReader)reader);
                user.Role = MapRoleFromReader((MySqlDataReader)reader, "RoleId", "RoleName", "RoleDescription");
                return user;
            }
            return null;
        }

        public async Task<IEnumerable<User>> GetAllAsync()
        {
            var users = new List<User>();

            using var connection = _connectionFactory.CreateConnection();
            await connection.OpenAsync();

            const string sql = @"
                SELECT u.*, r.Id as RoleId, r.Name as RoleName, r.Description as RoleDescription
                FROM Users u
                INNER JOIN Roles r ON u.RoleId = r.Id
                ORDER BY u.Id";

            using var cmd = new MySqlCommand(sql, connection);
            using var reader = await cmd.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                var user = MapUserFromReader((MySqlDataReader)reader);
                user.Role = MapRoleFromReader((MySqlDataReader)reader, "RoleId", "RoleName", "RoleDescription");
                users.Add(user);
            }

            return users;
        }

        public async Task<User> CreateUserWithGuestProfileAsync(User user, GuestProfile guestProfile)
        {
            using var connection = _connectionFactory.CreateConnection();
            await connection.OpenAsync();
            using var transaction = await connection.BeginTransactionAsync();

            try
            {
                // 1. Создаем пользователя
                const string insertUserSql = @"
                    INSERT INTO Users (Email, PasswordHash, Phone, RoleId, IsActive, CreatedAt) 
                    VALUES (@Email, @PasswordHash, @Phone, @RoleId, @IsActive, @CreatedAt);
                    SELECT LAST_INSERT_ID();";

                uint userId;
                using (var cmd = new MySqlCommand(insertUserSql, connection, transaction))
                {
                    cmd.Parameters.AddWithValue("@Email", user.Email);
                    cmd.Parameters.AddWithValue("@PasswordHash", user.PasswordHash);
                    cmd.Parameters.AddWithValue("@Phone", user.Phone);
                    cmd.Parameters.AddWithValue("@RoleId", user.RoleId);
                    cmd.Parameters.AddWithValue("@IsActive", user.IsActive);
                    cmd.Parameters.AddWithValue("@CreatedAt", DateTime.Now);

                    userId = Convert.ToUInt32(await cmd.ExecuteScalarAsync());
                }

                // 2. Создаем профиль гостя
                const string insertProfileSql = @"
                    INSERT INTO GuestProfiles (UserId, LastName, FirstName, MiddleName, PassportSeries, PassportNumber, Citizenship) 
                    VALUES (@UserId, @LastName, @FirstName, @MiddleName, @PassportSeries, @PassportNumber, @Citizenship);";

                using (var cmd = new MySqlCommand(insertProfileSql, connection, transaction))
                {
                    cmd.Parameters.AddWithValue("@UserId", userId);
                    cmd.Parameters.AddWithValue("@LastName", guestProfile.LastName);
                    cmd.Parameters.AddWithValue("@FirstName", guestProfile.FirstName);
                    cmd.Parameters.AddWithValue("@MiddleName", guestProfile.MiddleName ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@PassportSeries", guestProfile.PassportSeries);
                    cmd.Parameters.AddWithValue("@PassportNumber", guestProfile.PassportNumber);
                    cmd.Parameters.AddWithValue("@Citizenship", guestProfile.Citizenship);

                    await cmd.ExecuteNonQueryAsync();
                }

                await transaction.CommitAsync();

                user.Id = userId;
                _logger.LogInformation($"Guest user created: {user.Email}");
                return user;
            }
            catch (Exception ex)
            {
                await transaction.RollbackAsync();
                _logger.LogError(ex, "Failed to create guest user");
                throw;
            }
        }

        public async Task<User> CreateUserWithEmployeeProfileAsync(User user, EmployeeProfile employeeProfile)
        {
            using var connection = _connectionFactory.CreateConnection();
            await connection.OpenAsync();
            using var transaction = await connection.BeginTransactionAsync();

            try
            {
                // 1. Создаем пользователя
                const string insertUserSql = @"
                    INSERT INTO Users (Email, PasswordHash, Phone, RoleId, IsActive, CreatedAt) 
                    VALUES (@Email, @PasswordHash, @Phone, @RoleId, @IsActive, @CreatedAt);
                    SELECT LAST_INSERT_ID();";

                uint userId;
                using (var cmd = new MySqlCommand(insertUserSql, connection, transaction))
                {
                    cmd.Parameters.AddWithValue("@Email", user.Email);
                    cmd.Parameters.AddWithValue("@PasswordHash", user.PasswordHash);
                    cmd.Parameters.AddWithValue("@Phone", user.Phone);
                    cmd.Parameters.AddWithValue("@RoleId", user.RoleId);
                    cmd.Parameters.AddWithValue("@IsActive", user.IsActive);
                    cmd.Parameters.AddWithValue("@CreatedAt", DateTime.Now);

                    userId = Convert.ToUInt32(await cmd.ExecuteScalarAsync());
                }

                // 2. Создаем профиль сотрудника
                const string insertProfileSql = @"
                    INSERT INTO EmployeeProfiles (UserId, LastName, FirstName, MiddleName, Position, HireDate, Salary) 
                    VALUES (@UserId, @LastName, @FirstName, @MiddleName, @Position, @HireDate, @Salary);";

                using (var cmd = new MySqlCommand(insertProfileSql, connection, transaction))
                {
                    cmd.Parameters.AddWithValue("@UserId", userId);
                    cmd.Parameters.AddWithValue("@LastName", employeeProfile.LastName);
                    cmd.Parameters.AddWithValue("@FirstName", employeeProfile.FirstName);
                    cmd.Parameters.AddWithValue("@MiddleName", employeeProfile.MiddleName ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@Position", employeeProfile.Position);
                    cmd.Parameters.AddWithValue("@HireDate", employeeProfile.HireDate);
                    cmd.Parameters.AddWithValue("@Salary", employeeProfile.Salary ?? (object)DBNull.Value);

                    await cmd.ExecuteNonQueryAsync();
                }

                await transaction.CommitAsync();

                user.Id = userId;
                _logger.LogInformation($"Employee user created: {user.Email}");
                return user;
            }
            catch (Exception ex)
            {
                await transaction.RollbackAsync();
                _logger.LogError(ex, "Failed to create employee user");
                throw;
            }
        }

        public async Task<User> UpdateUserAsync(User user)
        {
            using var connection = _connectionFactory.CreateConnection();
            await connection.OpenAsync();

            const string sql = @"
                UPDATE Users 
                SET Email = @Email, 
                    Phone = @Phone, 
                    RoleId = @RoleId, 
                    IsActive = @IsActive
                WHERE Id = @Id";

            using var cmd = new MySqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@Email", user.Email);
            cmd.Parameters.AddWithValue("@Phone", user.Phone);
            cmd.Parameters.AddWithValue("@RoleId", user.RoleId);
            cmd.Parameters.AddWithValue("@IsActive", user.IsActive);
            cmd.Parameters.AddWithValue("@Id", user.Id);

            await cmd.ExecuteNonQueryAsync();

            _logger.LogInformation($"User updated: {user.Email}");
            return user;
        }

        public async Task<bool> DeleteUserAsync(uint id)
        {
            using var connection = _connectionFactory.CreateConnection();
            await connection.OpenAsync();

            // Профили удалятся автоматически из-за ON DELETE CASCADE
            const string sql = "DELETE FROM Users WHERE Id = @Id";
            using var cmd = new MySqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@Id", id);

            var affected = await cmd.ExecuteNonQueryAsync();

            if (affected > 0)
            {
                _logger.LogInformation($"User deleted: Id = {id}");
            }

            return affected > 0;
        }

        #endregion

        #region Profile Management

        public async Task<GuestProfile> UpdateGuestProfileAsync(GuestProfile profile)
        {
            using var connection = _connectionFactory.CreateConnection();
            await connection.OpenAsync();

            const string sql = @"
                UPDATE GuestProfiles 
                SET LastName = @LastName,
                    FirstName = @FirstName,
                    MiddleName = @MiddleName,
                    PassportSeries = @PassportSeries,
                    PassportNumber = @PassportNumber,
                    Citizenship = @Citizenship
                WHERE UserId = @UserId";

            using var cmd = new MySqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@LastName", profile.LastName);
            cmd.Parameters.AddWithValue("@FirstName", profile.FirstName);
            cmd.Parameters.AddWithValue("@MiddleName", profile.MiddleName ?? (object)DBNull.Value);
            cmd.Parameters.AddWithValue("@PassportSeries", profile.PassportSeries);
            cmd.Parameters.AddWithValue("@PassportNumber", profile.PassportNumber);
            cmd.Parameters.AddWithValue("@Citizenship", profile.Citizenship);
            cmd.Parameters.AddWithValue("@UserId", profile.UserId);

            await cmd.ExecuteNonQueryAsync();

            _logger.LogInformation($"Guest profile updated for UserId: {profile.UserId}");
            return profile;
        }

        public async Task<EmployeeProfile> UpdateEmployeeProfileAsync(EmployeeProfile profile)
        {
            using var connection = _connectionFactory.CreateConnection();
            await connection.OpenAsync();

            const string sql = @"
                UPDATE EmployeeProfiles 
                SET LastName = @LastName,
                    FirstName = @FirstName,
                    MiddleName = @MiddleName,
                    Position = @Position,
                    HireDate = @HireDate,
                    Salary = @Salary
                WHERE UserId = @UserId";

            using var cmd = new MySqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@LastName", profile.LastName);
            cmd.Parameters.AddWithValue("@FirstName", profile.FirstName);
            cmd.Parameters.AddWithValue("@MiddleName", profile.MiddleName ?? (object)DBNull.Value);
            cmd.Parameters.AddWithValue("@Position", profile.Position);
            cmd.Parameters.AddWithValue("@HireDate", profile.HireDate);
            cmd.Parameters.AddWithValue("@Salary", profile.Salary ?? (object)DBNull.Value);
            cmd.Parameters.AddWithValue("@UserId", profile.UserId);

            await cmd.ExecuteNonQueryAsync();

            _logger.LogInformation($"Employee profile updated for UserId: {profile.UserId}");
            return profile;
        }

        public async Task<GuestProfile?> GetGuestProfileByUserIdAsync(uint userId)
        {
            using var connection = _connectionFactory.CreateConnection();
            await connection.OpenAsync();

            const string sql = "SELECT * FROM GuestProfiles WHERE UserId = @UserId";
            using var cmd = new MySqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@UserId", userId);

            using var reader = await cmd.ExecuteReaderAsync();
            if (await reader.ReadAsync())
            {
                return MapGuestProfileFromReader(   (MySqlDataReader)reader);
            }
            return null;
        }

        public async Task<EmployeeProfile?> GetEmployeeProfileByUserIdAsync(uint userId)
        {
            using var connection = _connectionFactory.CreateConnection();
            await connection.OpenAsync();

            const string sql = "SELECT * FROM EmployeeProfiles WHERE UserId = @UserId";
            using var cmd = new MySqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@UserId", userId);

            using var reader = await cmd.ExecuteReaderAsync();
            if (await reader.ReadAsync())
            {
                return MapEmployeeProfileFromReader((MySqlDataReader)reader);
            }
            return null;
        }

        #endregion

        #region Search and Filter

        public async Task<IEnumerable<User>> GetUsersByRoleAsync(string roleName)
        {
            var users = new List<User>();

            using var connection = _connectionFactory.CreateConnection();
            await connection.OpenAsync();

            const string sql = @"
                SELECT u.*, r.Id as RoleId, r.Name as RoleName, r.Description as RoleDescription
                FROM Users u
                INNER JOIN Roles r ON u.RoleId = r.Id
                WHERE r.Name = @RoleName
                ORDER BY u.Id";

            using var cmd = new MySqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@RoleName", roleName);

            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                var user = MapUserFromReader((MySqlDataReader)reader);
                user.Role = MapRoleFromReader((MySqlDataReader)reader, "RoleId", "RoleName", "RoleDescription");
                users.Add(user);
            }

            return users;
        }

        public async Task<IEnumerable<User>> SearchUsersAsync(string searchTerm)
        {
            var users = new List<User>();

            using var connection = _connectionFactory.CreateConnection();
            await connection.OpenAsync();

            const string sql = @"
                SELECT u.*, r.Id as RoleId, r.Name as RoleName, r.Description as RoleDescription
                FROM Users u
                INNER JOIN Roles r ON u.RoleId = r.Id
                WHERE u.Email LIKE @SearchTerm 
                   OR u.Phone LIKE @SearchTerm
                   OR EXISTS (SELECT 1 FROM GuestProfiles gp WHERE gp.UserId = u.Id AND (gp.LastName LIKE @SearchTerm OR gp.FirstName LIKE @SearchTerm))
                   OR EXISTS (SELECT 1 FROM EmployeeProfiles ep WHERE ep.UserId = u.Id AND (ep.LastName LIKE @SearchTerm OR ep.FirstName LIKE @SearchTerm))
                ORDER BY u.Id";

            var term = $"%{searchTerm}%";
            using var cmd = new MySqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@SearchTerm", term);

            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                var user = MapUserFromReader((MySqlDataReader)reader);
                user.Role = MapRoleFromReader((MySqlDataReader)reader, "RoleId", "RoleName", "RoleDescription");
                users.Add(user);
            }

            return users;
        }

        #endregion

        #region Authentication

        public async Task<User?> AuthenticateAsync(string email, string password)
        {
            var user = await GetByEmailAsync(email);

            if (user == null || !user.IsActive)
                return null;

            // В реальном приложении используйте хеширование
            if (user.PasswordHash != password)
                return null;

            return user;
        }

        public async Task<bool> UpdateLastLoginAsync(uint userId)
        {
            using var connection = _connectionFactory.CreateConnection();
            await connection.OpenAsync();

            const string sql = "UPDATE Users SET LastLoginAt = NOW() WHERE Id = @Id";
            using var cmd = new MySqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@Id", userId);

            var affected = await cmd.ExecuteNonQueryAsync();
            return affected > 0;
        }

        public async Task<bool> ChangePasswordAsync(uint userId, string newPasswordHash)
        {
            using var connection = _connectionFactory.CreateConnection();
            await connection.OpenAsync();

            const string sql = "UPDATE Users SET PasswordHash = @PasswordHash WHERE Id = @Id";
            using var cmd = new MySqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@PasswordHash", newPasswordHash);
            cmd.Parameters.AddWithValue("@Id", userId);

            var affected = await cmd.ExecuteNonQueryAsync();

            if (affected > 0)
            {
                _logger.LogInformation($"Password changed for user Id: {userId}");
            }

            return affected > 0;
        }

        #endregion

        #region Existence Checks

        public async Task<bool> EmailExistsAsync(string email, uint? excludeId = null)
        {
            using var connection = _connectionFactory.CreateConnection();
            await connection.OpenAsync();

            string sql;
            if (excludeId.HasValue)
            {
                sql = "SELECT COUNT(*) > 0 FROM Users WHERE Email = @Email AND Id != @ExcludeId";
            }
            else
            {
                sql = "SELECT COUNT(*) > 0 FROM Users WHERE Email = @Email";
            }

            using var cmd = new MySqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@Email", email);
            if (excludeId.HasValue)
            {
                cmd.Parameters.AddWithValue("@ExcludeId", excludeId.Value);
            }

            return Convert.ToBoolean(await cmd.ExecuteScalarAsync());
        }

        public async Task<bool> PhoneExistsAsync(string phone, uint? excludeId = null)
        {
            using var connection = _connectionFactory.CreateConnection();
            await connection.OpenAsync();

            string sql;
            if (excludeId.HasValue)
            {
                sql = "SELECT COUNT(*) > 0 FROM Users WHERE Phone = @Phone AND Id != @ExcludeId";
            }
            else
            {
                sql = "SELECT COUNT(*) > 0 FROM Users WHERE Phone = @Phone";
            }

            using var cmd = new MySqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@Phone", phone);
            if (excludeId.HasValue)
            {
                cmd.Parameters.AddWithValue("@ExcludeId", excludeId.Value);
            }

            return Convert.ToBoolean(await cmd.ExecuteScalarAsync());
        }

        public async Task<bool> PassportExistsAsync(string series, string number, uint? excludeUserId = null)
        {
            using var connection = _connectionFactory.CreateConnection();
            await connection.OpenAsync();

            string sql;
            if (excludeUserId.HasValue)
            {
                sql = "SELECT COUNT(*) > 0 FROM GuestProfiles WHERE PassportSeries = @Series AND PassportNumber = @Number AND UserId != @ExcludeUserId";
            }
            else
            {
                sql = "SELECT COUNT(*) > 0 FROM GuestProfiles WHERE PassportSeries = @Series AND PassportNumber = @Number";
            }

            using var cmd = new MySqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@Series", series);
            cmd.Parameters.AddWithValue("@Number", number);
            if (excludeUserId.HasValue)
            {
                cmd.Parameters.AddWithValue("@ExcludeUserId", excludeUserId.Value);
            }

            return Convert.ToBoolean(await cmd.ExecuteScalarAsync());
        }

        #endregion

        #region Statistics

        public async Task<int> GetCountAsync()
        {
            using var connection = _connectionFactory.CreateConnection();
            await connection.OpenAsync();

            const string sql = "SELECT COUNT(*) FROM Users";
            using var cmd = new MySqlCommand(sql, connection);

            return Convert.ToInt32(await cmd.ExecuteScalarAsync());
        }

        public async Task<Dictionary<string, int>> GetUsersByRoleStatsAsync()
        {
            var stats = new Dictionary<string, int>();

            using var connection = _connectionFactory.CreateConnection();
            await connection.OpenAsync();

            const string sql = @"
                SELECT r.Name as RoleName, COUNT(u.Id) as UserCount
                FROM Users u
                INNER JOIN Roles r ON u.RoleId = r.Id
                GROUP BY r.Name";

            using var cmd = new MySqlCommand(sql, connection);
            using var reader = await cmd.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                var roleName = reader.GetString("RoleName");
                var count = reader.GetInt32("UserCount");
                stats.Add(roleName, count);
            }

            return stats;
        }

        #endregion

        #region Roles

        public async Task<IEnumerable<Role>> GetAllRolesAsync()
        {
            var roles = new List<Role>();

            using var connection = _connectionFactory.CreateConnection();
            await connection.OpenAsync();

            const string sql = "SELECT * FROM Roles ORDER BY Id";
            using var cmd = new MySqlCommand(sql, connection);
            using var reader = await cmd.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                roles.Add(MapRoleFromReader((MySqlDataReader)reader));
            }

            return roles;
        }

        public async Task<Role?> GetRoleByNameAsync(string name)
        {
            using var connection = _connectionFactory.CreateConnection();
            await connection.OpenAsync();

            const string sql = "SELECT * FROM Roles WHERE Name = @Name";
            using var cmd = new MySqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@Name", name);

            using var reader = await cmd.ExecuteReaderAsync();
            if (await reader.ReadAsync())
            {
                return MapRoleFromReader((MySqlDataReader)reader);
            }
            return null;
        }

        #endregion

        #region Mapping Methods

        private User MapUserFromReader(MySqlDataReader reader)
        {
            return new User
            {
                Id = reader.GetUInt32("Id"),
                Email = reader.GetString("Email"),
                PasswordHash = reader.GetString("PasswordHash"),
                Phone = reader.GetString("Phone"),
                RoleId = reader.GetUInt32("RoleId"),
                IsActive = reader.GetBoolean("IsActive"),
                CreatedAt = reader.GetDateTime("CreatedAt"),
                LastLoginAt = reader.IsDBNull(reader.GetOrdinal("LastLoginAt"))
                    ? null : reader.GetDateTime("LastLoginAt")
            };
        }

        private Role MapRoleFromReader(MySqlDataReader reader, string idColumn = "Id", string nameColumn = "Name", string descriptionColumn = "Description")
        {
            return new Role
            {
                Id = reader.GetUInt32(idColumn),
                Name = reader.GetString(nameColumn),
                Description = reader.IsDBNull(reader.GetOrdinal(descriptionColumn))
                    ? null : reader.GetString(descriptionColumn)
            };
        }

        private GuestProfile MapGuestProfileFromReader(MySqlDataReader reader)
        {
            return new GuestProfile
            {
                UserId = reader.GetUInt32("UserId"),
                LastName = reader.GetString("LastName"),
                FirstName = reader.GetString("FirstName"),
                MiddleName = reader.IsDBNull(reader.GetOrdinal("MiddleName"))
                    ? null : reader.GetString("MiddleName"),
                PassportSeries = reader.GetString("PassportSeries"),
                PassportNumber = reader.GetString("PassportNumber"),
                Citizenship = reader.GetString("Citizenship")
            };
        }

        private EmployeeProfile MapEmployeeProfileFromReader(MySqlDataReader reader)
        {
            return new EmployeeProfile
            {
                UserId = reader.GetUInt32("UserId"),
                LastName = reader.GetString("LastName"),
                FirstName = reader.GetString("FirstName"),
                MiddleName = reader.IsDBNull(reader.GetOrdinal("MiddleName"))
                    ? null : reader.GetString("MiddleName"),
                Position = reader.GetString("Position"),
                HireDate = reader.GetDateTime("HireDate"),
                Salary = reader.IsDBNull(reader.GetOrdinal("Salary"))
                    ? null : reader.GetDecimal("Salary")
            };
        }

        #endregion
    }
}
