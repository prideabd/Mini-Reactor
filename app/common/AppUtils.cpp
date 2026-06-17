#include <unordered_map>

#include "AppUtils.h"

namespace app {

std::string GetMimeType(const std::string& path) {
    static const std::unordered_map<std::string, std::string> mime_types = {
        {".html", "text/html; charset=utf-8"},
        {".css", "text/css; charset=utf-8"},
        {".js", "application/javascript; charset=utf-8"},
        {".png", "image/png"},
        {".jpg", "image/jpeg"},
        {".gif", "image/gif"},
        {".ico", "image/x-icon"},
        {".svg", "image/svg+xml"},
        {".json", "application/json; charset=utf-8"}
    };
    size_t dot_pos = path.find_last_of('.');
    if (dot_pos != std::string::npos) {
        std::string ext = path.substr(dot_pos);
        auto it = mime_types.find(ext);
        if (it != mime_types.end()) {
            return it->second;
        }
    }
    return "text/plain; charset=utf-8"; // 默认
}

std::string UrlDecode(const std::string& str) {
    std::string result;
    result.reserve(str.size()); // 优化内存分配，榨干高并发下的吞吐性能
    
    for (size_t i = 0; i < str.length(); ++i) {
        if (str[i] == '+') {
            result += ' '; // 标准表单规范：将 '+' 还原为空格
        } else if (str[i] == '%' && i + 2 < str.length()) {
            // 物理把两个十六进制字符（如 E4）拼成一个真实字节
            char high = str[i + 1];
            char low = str[i + 2];
            
            auto HexToChar = [](char c) -> int {
                if (c >= 'A' && c <= 'F') return c - 'A' + 10;
                if (c >= 'a' && c <= 'f') return c - 'a' + 10;
                if (c >= '0' && c <= '9') return c - '0';
                return 0;
            };
            
            int byte_val = (HexToChar(high) << 4) + HexToChar(low);
            result += static_cast<char>(byte_val);
            i += 2; // 指针前移，跳过已经消费的两个十六进制位
        } else {
            result += str[i];
        }
    }
    return result;
}

}// namespace app