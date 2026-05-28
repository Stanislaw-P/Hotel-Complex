using HotelComplex.Db.Models;

namespace HotelComplex.Db.Repositories
{
    public interface IUserRepository
    {
        // Основные CRUD для пользователей
        Task<User?> GetByIdAsync(uint id);
        Task<User?> GetByEmailAsync(string email);
        Task<IEnumerable<User>> GetAllAsync();
        Task<User> CreateUserWithGuestProfileAsync(User user, GuestProfile guestProfile);
        Task<User> CreateUserWithEmployeeProfileAsync(User user, EmployeeProfile employeeProfile);
        Task<User> UpdateUserAsync(User user);
        Task<bool> DeleteUserAsync(uint id);

        // Обновление профилей
        Task<GuestProfile> UpdateGuestProfileAsync(GuestProfile profile);
        Task<EmployeeProfile> UpdateEmployeeProfileAsync(EmployeeProfile profile);

        // Получение профилей
        Task<GuestProfile?> GetGuestProfileByUserIdAsync(uint userId);
        Task<EmployeeProfile?> GetEmployeeProfileByUserIdAsync(uint userId);

        // Поиск
        Task<IEnumerable<User>> GetUsersByRoleAsync(string roleName);
        Task<IEnumerable<User>> SearchUsersAsync(string searchTerm);

        // Аутентификация
        Task<User?> AuthenticateAsync(string email, string password);
        Task<bool> UpdateLastLoginAsync(uint userId);
        Task<bool> ChangePasswordAsync(uint userId, string newPasswordHash);

        // Проверки существования
        Task<bool> EmailExistsAsync(string email, uint? excludeId = null);
        Task<bool> PhoneExistsAsync(string phone, uint? excludeId = null);
        Task<bool> PassportExistsAsync(string series, string number, uint? excludeUserId = null);

        // Статистика
        Task<int> GetCountAsync();
        Task<Dictionary<string, int>> GetUsersByRoleStatsAsync();

        // Получение всех ролей
        Task<IEnumerable<Role>> GetAllRolesAsync();
        Task<Role?> GetRoleByNameAsync(string name);
    }
}
