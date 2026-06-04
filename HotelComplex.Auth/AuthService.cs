using System.Net.Http;
using System.Security.Claims;

namespace HotelComplex.Auth
{
    public class AuthService
    {
        private readonly IHttpContextAccessor _httpContextAccessor;
        private readonly IUserRepository _userRepository;
        private readonly IPasswordHasher _passwordHasher;

        public AuthService(
            IHttpContextAccessor httpContextAccessor,
            IUserRepository userRepository,
            IPasswordHasher passwordHasher)
        {
            _httpContextAccessor = httpContextAccessor;
            _userRepository = userRepository;
            _passwordHasher = passwordHasher;
        }

        private HttpContext HttpContext => _httpContextAccessor.HttpContext
            ?? throw new InvalidOperationException("HttpContext not available");

        public bool IsAuthenticated => HttpContext.User.Identity?.IsAuthenticated ?? false;

        public uint? CurrentUserId
        {
            get
            {
                var claim = HttpContext.User.FindFirst(ClaimTypes.NameIdentifier);
                return claim != null ? uint.Parse(claim.Value) : null;
            }
        }

        public string? CurrentUserEmail => HttpContext.User.FindFirst(ClaimTypes.Email)?.Value;

        public string? CurrentUserName => HttpContext.User.FindFirst(ClaimTypes.Name)?.Value;

        public string? CurrentUserRole => HttpContext.User.FindFirst(ClaimTypes.Role)?.Value;

        public async Task<bool> LoginAsync(string email, string password, bool rememberMe)
        {
            var user = await _userRepository.GetByEmailAsync(email);

            if (user == null || !user.IsActive)
                return false;

            if (!_passwordHasher.VerifyPassword(password, user.PasswordHash))
                return false;

            // Получаем имя пользователя
            var userName = await GetUserNameAsync(user.Id);
            var roleName = user.Role?.Name ?? "User";

            // Создаем claims
            var claims = new List<Claim>
            {
                new Claim(ClaimTypes.NameIdentifier, user.Id.ToString()),
                new Claim(ClaimTypes.Email, user.Email),
                new Claim(ClaimTypes.Name, userName),
                new Claim(ClaimTypes.Role, roleName)
            };

            var claimsIdentity = new ClaimsIdentity(claims, CookieAuthenticationDefaults.AuthenticationScheme);
            var authProperties = new AuthenticationProperties
            {
                IsPersistent = rememberMe,
                ExpiresUtc = rememberMe ? DateTimeOffset.UtcNow.AddDays(14) : DateTimeOffset.UtcNow.AddDays(1)
            };

            await HttpContext.SignInAsync(
                CookieAuthenticationDefaults.AuthenticationScheme,
                new ClaimsPrincipal(claimsIdentity),
                authProperties);

            // Обновляем время последнего входа
            await _userRepository.UpdateLastLoginAsync(user.Id);

            return true;
        }

        public async Task<RegistrationResult> RegisterAsync(RegisterViewModel model)
        {
            // Проверка существования email
            if (await _userRepository.EmailExistsAsync(model.Email))
            {
                return RegistrationResult.Fail("Пользователь с таким email уже существует");
            }

            // Проверка существования телефона
            if (await _userRepository.PhoneExistsAsync(model.Phone))
            {
                return RegistrationResult.Fail("Пользователь с таким телефоном уже существует");
            }

            // Проверка существования паспорта (для гостей)
            if (!string.IsNullOrEmpty(model.PassportSeries) && !string.IsNullOrEmpty(model.PassportNumber))
            {
                if (await _userRepository.PassportExistsAsync(model.PassportSeries, model.PassportNumber))
                {
                    return RegistrationResult.Fail("Гость с таким паспортом уже зарегистрирован");
                }
            }

            // Получаем роль "User"
            var userRole = await _userRepository.GetRoleByNameAsync("User");
            if (userRole == null)
            {
                return RegistrationResult.Fail("Системная ошибка: роль не найдена");
            }

            // Создаем пользователя
            var user = new User
            {
                Email = model.Email,
                PasswordHash = _passwordHasher.HashPassword(model.Password),
                Phone = model.Phone,
                RoleId = userRole.Id,
                IsActive = true,
                CreatedAt = DateTime.Now
            };

            // Создаем профиль гостя
            var guestProfile = new GuestProfile
            {
                LastName = model.LastName,
                FirstName = model.FirstName,
                MiddleName = model.MiddleName,
                PassportSeries = model.PassportSeries ?? "",
                PassportNumber = model.PassportNumber ?? "",
                Citizenship = "РФ"
            };

            try
            {
                await _userRepository.CreateUserWithGuestProfileAsync(user, guestProfile);
                return RegistrationResult.Ok();
            }
            catch (Exception ex)
            {
                return RegistrationResult.Fail("Ошибка при создании аккаунта. Попробуйте позже.");
            }
        }

        public async Task LogoutAsync()
        {
            await HttpContext.SignOutAsync(CookieAuthenticationDefaults.AuthenticationScheme);
        }

        public async Task<User?> GetCurrentUserAsync()
        {
            if (!CurrentUserId.HasValue)
                return null;

            var user = await _userRepository.GetByIdAsync(CurrentUserId.Value);
            if (user != null)
            {
                user.GuestProfile = await _userRepository.GetGuestProfileByUserIdAsync(user.Id);
                user.EmployeeProfile = await _userRepository.GetEmployeeProfileByUserIdAsync(user.Id);
            }
            return user;
        }

        public async Task<bool> IsInRoleAsync(string role)
        {
            return await Task.FromResult(CurrentUserRole == role);
        }

        private async Task<string> GetUserNameAsync(uint userId)
        {
            var guestProfile = await _userRepository.GetGuestProfileByUserIdAsync(userId);
            if (guestProfile != null)
            {
                return guestProfile.FullName;
            }

            var employeeProfile = await _userRepository.GetEmployeeProfileByUserIdAsync(userId);
            if (employeeProfile != null)
            {
                return employeeProfile.FullName;
            }

            return "Пользователь";
        }
    }
}
