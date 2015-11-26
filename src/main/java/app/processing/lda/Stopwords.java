package app.processing.lda;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class Stopwords {

	public static String[] stopwords = { "nhận", "rằng", "cao", "nhà", "quá",
			"riêng", "gì", "muốn", "muon", "rồi", "số", "thấy", "hay", "lên",
			"lần", "nào", "qua", "bằng", "điều", "biết", "lớn", "khác", "vừa",
			"nếu", "thời_gian", "họ", "từng", "đây", "tháng", "trước", "chính",
			"cả", "việc", "chưa", "do", "nói", "ra", "nên", "đều", "đi", "tới",
			"tôi", "có_thể", "cùng", "vì", "làm", "lại", "mới", "ngày", "đó",
			"vẫn", "mình", "chỉ", "thì", "đang", "còn", "bị", "mà", "năm",
			"nhất", "hơn", "sau", "ông", "rất", "anh", "phải", "như", "trên",
			"tại", "theo", "khi", "nhưng", "vào", "đến", "nhiều", "người",
			"từ", "sẽ", "ở", "cũng", "không", "về", "để", "này", "những",
			"một", "các", "cho", "được", "với", "có", "trong", "đã", "là",
			"và", "của", "thực_sự", "ở_trên", "tất_cả", "dưới", "hầu_hết",
			"luôn", "giữa", "bất_kỳ", "hỏi", "bạn", "cô", "tôi", "tớ", "cậu",
			"bác", "chú", "dì", "thím", "cậu", "mợ", "ông", "bà", "em",
			"thường", "ai", "cảm_ơn", "bởi", "cái", "cần", "càng", "chiếc",
			"chứ", "chuyện", "cứ", "đến_nỗi", "một cách", "so", "so_sánh",
			"thật", "đúng", "hiểu", "dành", "thôi", "chẳng", "nghĩ", "viết",
			"xuống", "thớt", "nhìn", "đường", "chắc", "mong", "đừng", "nhau",
			"chào", "xin_chào", "như_vậy", "đăng", "nghe", "giúp", "trường",
			"ngồi", "chơi", "chung", "khoa", "pupu", "cách", "buổi", "sáng",
			"bao_giờ", "thật_sự", "anh_chị", "tiếng", "from", "quên", "ngoài",
			"phòng", "thêm", "ngay", "đứng", "tuần", "typn", "hoặc", "thằng",
			"ngành", "đá", "rio", "cừu", "liên_hệ", "đầu_tiên", "đại_học",
			"thông_tin", "chạy", "sinh", "nắng", "mang", "hình", "vui_lòng",
			"hôm_nay", "confession", "kiểu", "tính", "ngta", "quen", "đóng",
			"lòng", "nàng", "chẳng_hạn", "rõ_rệt", "cho_biết", "tuổi", "ngừng",
			"đồng", "nước", "hàng", "triệu", "cho_biết", "cuộc", "bờ_rào",
			"hình_như", "khóa", "mềnh", "loại", "chọn", "chúng_ta", "cảm_thấy",
			"làm_sao", "khỏi", "bóng", "nghỉ", "vượt", "có_lẽ", "thành",
			"thời", "cuốn", "chuyển", "bước", "bảng", "thế_này", "hoài",
			"xong", "khoảng", "thay", "cũng_như", "phần", "minh", "rưỡi",
			"mà_còn", "tiện", "chừng", "canh", "điền", "thui", "tưởng",
			"thế_nào", "như_thế_nào", "như_thế", "tầng" };

	public static Set<String> stopWordSet = new HashSet<String>(
			Arrays.asList(stopwords));

	public static boolean isStopword(String word) {
		if (word.length() < 2)
			return true;
		// if (word.charAt(0) >= '0' && word.charAt(0) <= '9')
		// return true; // remove numbers, "25th", etc
		if (stopWordSet.contains(word.toLowerCase()))
			return true;
		else
			return false;
	}

	public static String removeStopWords(String string) {
		String result = "";
		String[] words = string.split("\\s+");
		for (String word : words) {
			if (word.isEmpty())
				continue;
			if (isStopword(string))
				continue; // remove stopwords
			result += (word + " ");
		}
		return result;
	}
}
