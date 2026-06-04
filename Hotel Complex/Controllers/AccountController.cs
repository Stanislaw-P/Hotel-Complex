using Hotel_Complex.Models;
using Hotel_Complex.Services;
using Microsoft.AspNetCore.Mvc;

namespace Hotel_Complex.Controllers
{
    public class AccountController : Controller
    {
        private readonly AuthService _authService;
        private readonly ILogger<AccountController> _logger;

        public AccountController(AuthService authService, ILogger<AccountController> logger)
        {
            _authService = authService;
            _logger = logger;
        }

        public IActionResult Login()
        {
            if (_authService.IsAuthenticated)
                return RedirectToAction("Index", "PublicHome");

            return View();
        }

        [HttpPost]
        public async Task<IActionResult> LoginAsync(LoginViewModel model)
        {
            if (ModelState.IsValid)
            {
                var result = await _authService.LoginAsync(model.Email, model.Password, model.RememberMe);

                if (result)
                {
                    _logger.LogInformation($"User {model.Email} logged in successfully");

                    if (_authService.CurrentUserRole == "Admin")
                    {
                        return RedirectToAction("Index", "AdminHome");
                    }

                    return RedirectToAction("Index", "PublicHome");
                }

                ModelState.AddModelError("", "Неверный email или пароль");
            }

            return View(model);
        }

        public IActionResult Register()
        {
            if (_authService.IsAuthenticated)
                return RedirectToAction("Index", "PublicHome");
            return View();
        }

        [HttpPost]
        public async Task<IActionResult> Register(RegisterViewModel model)
        {
            if (ModelState.IsValid)
            {
                var result = await _authService.RegisterAsync(model);

                if (result.Success)
                {
                    TempData["Success"] = "Регистрация прошла успешно! Теперь вы можете войти.";
                    return RedirectToAction(nameof(Login));
                }

                foreach (var error in result.Errors)
                {
                    ModelState.AddModelError("", error);
                }
            }

            return View(model);
        }

        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Logout()
        {
            await _authService.LogoutAsync();
            return RedirectToAction("Index", "PublicHome");
        }

        [HttpGet]
        public IActionResult AccessDenied()
        {
            return View();
        }
    }
}
