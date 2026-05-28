using HotelComplex.Db.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HotelComplex.Db.Repositories
{
    public class UserRepository : IUserRepository
    {
        public Task<User?> AuthenticateAsync(string email, string password)
        {
            throw new NotImplementedException();
        }

        public Task<bool> ChangePasswordAsync(uint id, string newPasswordHash)
        {
            throw new NotImplementedException();
        }

        public Task<bool> ChangeStatusAsync(uint id, bool isActive)
        {
            throw new NotImplementedException();
        }

        public Task<User> CreateAsync(User user)
        {
            throw new NotImplementedException();
        }

        public Task<bool> DeleteAsync(uint id)
        {
            throw new NotImplementedException();
        }

        public Task<bool> EmailExistsAsync(string email, uint? excludeId = null)
        {
            throw new NotImplementedException();
        }

        public Task<IEnumerable<User>> GetAllAsync()
        {
            throw new NotImplementedException();
        }

        public Task<User?> GetByEmailAsync(string email)
        {
            throw new NotImplementedException();
        }

        public Task<User?> GetByIdAsync(uint id)
        {
            throw new NotImplementedException();
        }

        public Task<int> GetCountAsync()
        {
            throw new NotImplementedException();
        }

        public Task<IEnumerable<User>> GetRecentUsersAsync(int count)
        {
            throw new NotImplementedException();
        }

        public Task<bool> PhoneExistsAsync(string phone, uint? excludeId = null)
        {
            throw new NotImplementedException();
        }

        public Task<User> UpdateAsync(User user)
        {
            throw new NotImplementedException();
        }

        public Task<bool> UpdateLastLoginAsync(uint id)
        {
            throw new NotImplementedException();
        }
    }
    public interface IUserRepository
    {
        // Основные CRUD
        Task<User?> GetByIdAsync(uint id);
        Task<User?> GetByEmailAsync(string email);
        Task<IEnumerable<User>> GetAllAsync();
        Task<User> CreateAsync(User user);
        Task<User> UpdateAsync(User user);
        Task<bool> DeleteAsync(uint id);

        // Аутентификация
        Task<User?> AuthenticateAsync(string email, string password);

        // Проверки существования
        Task<bool> EmailExistsAsync(string email, uint? excludeId = null);
        Task<bool> PhoneExistsAsync(string phone, uint? excludeId = null);

        // Обновление статусов
        Task<bool> UpdateLastLoginAsync(uint id);
        Task<bool> ChangePasswordAsync(uint id, string newPasswordHash);
        Task<bool> ChangeStatusAsync(uint id, bool isActive);

        // Статистика
        Task<int> GetCountAsync();
        Task<IEnumerable<User>> GetRecentUsersAsync(int count);
    }
}
