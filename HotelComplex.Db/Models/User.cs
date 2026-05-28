using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HotelComplex.Db.Models
{
    public class User
    {
        public uint Id { get; set; }
        public string Email { get; set; } = string.Empty;
        public string PasswordHash { get; set; } = string.Empty;
        public string Phone { get; set; } = string.Empty;
        public uint RoleId { get; set; }
        public bool IsActive { get; set; } = true;
        public DateTime CreatedAt { get; set; }
        public DateTime? LastLoginAt { get; set; }

        // Навигационные свойства
        public Role? Role { get; set; }
        public GuestProfile? GuestProfile { get; set; }
        public EmployeeProfile? EmployeeProfile { get; set; }

        // Вычисляемые свойства
        public string RoleName => Role?.Name ?? "Unknown";
        public bool IsGuest => GuestProfile != null;
        public bool IsEmployee => EmployeeProfile != null;
        public string FullName => IsGuest ? GuestProfile?.FullName ?? "" : EmployeeProfile?.FullName ?? "";
    }
}
