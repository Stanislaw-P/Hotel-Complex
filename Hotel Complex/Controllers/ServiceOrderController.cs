using Hotel_Complex.Models;
using Microsoft.AspNetCore.Mvc;

namespace Hotel_Complex.Controllers
{
    public class ServiceOrderController : Controller
    {
        private readonly ILogger<ServiceOrderController> _logger;

        public ServiceOrderController(ILogger<ServiceOrderController> logger)
        {
            _logger = logger;
        }

        [HttpGet]
        public IActionResult Index()
        {
            return View();
        }

        [HttpGet]
        public IActionResult Create(int stayId)
        {
            ViewBag.StayId = stayId;
            return View();
        }

        [HttpPost]
        public IActionResult Create(ServiceOrderViewModel model)
        {
            if (ModelState.IsValid)
            {
                _logger.LogInformation($"Заказ услуги: {model.ServiceName}, Стоимость: {model.Price} ₽");
                TempData["Success"] = "Услуга успешно заказана!";
                return RedirectToAction("Success");
            }
            return View(model);
        }

        [HttpGet]
        public IActionResult Success()
        {
            return View();
        }
    }
}
