#pragma once
#include <string>

namespace app {

    // 根据文件后缀名，自动匹配 HTTP 标准媒体类型（MIME Type）
    std::string GetMimeType(const std::string& path);
    
    // 高可用 URL 物理解码函数
    std::string UrlDecode(const std::string& str);

} // namespace app