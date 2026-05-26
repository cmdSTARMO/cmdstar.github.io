from fastapi import APIRouter

# 只引入你真正要启用的子模块
from . import szse_margin_data_total   # 20250809
from . import szse_margin_data_details  # 20250809
from . import sse_margin_data_total    # 20250809
from . import sse_margin_data_details   # 20250809
from . import margin_total_merged

# 父层：/margin
router = APIRouter(prefix="/margin", tags=["Margin Data"])

# 子层：/margin/szse
router.include_router(szse_margin_data_total.router, prefix="/szse") # , tags=["szse_margin_data_total"])
router.include_router(szse_margin_data_details.router, prefix="/szse") # , tags=["szse_margin_data_details"])

# 子层：/margin/sse
router.include_router(sse_margin_data_total.router, prefix="/sse") # , tags=["sse_margin_data_total"])
router.include_router(sse_margin_data_details.router, prefix="/sse") # , tags=["sse_margin_data_detail"])

# åˆå¹¶å±‚ï¼š/margin/merged
router.include_router(margin_total_merged.router, prefix="/merged")
