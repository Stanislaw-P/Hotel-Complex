using Microsoft.AspNetCore.Mvc;

namespace Hotel_Complex.Areas.Admin.Controllers
{
    [Area("Admin")]
    public class GuestsController : Controller
    {
        public IActionResult Index()
        {
            return View();
        }

        public IActionResult Create()
        {
            return View();
        }

        public IActionResult Details()
        {
            return View();
        }

        public IActionResult Edit()
        {
            return View();
        }
    }
}
