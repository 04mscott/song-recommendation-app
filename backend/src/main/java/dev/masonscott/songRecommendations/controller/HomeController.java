package dev.masonscott.songRecommendations.controller;

import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.client.OAuth2AuthorizedClient;
import org.springframework.security.oauth2.client.OAuth2AuthorizedClientService;
import org.springframework.security.oauth2.core.user.OAuth2User;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import java.security.Principal;
import java.util.Map;

@RestController
public class HomeController {

    private final OAuth2AuthorizedClientService authorizedClientService;

    public HomeController(OAuth2AuthorizedClientService authorizedClientService) {
        this.authorizedClientService = authorizedClientService;
    }

    @GetMapping("/")
    public String home() {
        return "Hello Home!";
    }

    @GetMapping("/secured")
    public String secured(@AuthenticationPrincipal OAuth2User principal) {
        OAuth2AuthorizedClient authorizedClient = authorizedClientService.loadAuthorizedClient(
                "spotify",
                principal.getName()
        );
        String accessToken = authorizedClient.getAccessToken().getTokenValue();
        System.out.println("ACCESS TOKEN: " + accessToken);
        return "Hello Secured! Token: " + accessToken;
    }

//    @GetMapping("/me")
//    public Map<String, Object> getUser(@AuthenticationPrincipal OAuth2User principal) {
//        return principal.getAttributes();
//    }

    @GetMapping("/recommend")
    public String getRecommendations(@AuthenticationPrincipal OAuth2User principal) {
        OAuth2AuthorizedClient authorizedClient = authorizedClientService.loadAuthorizedClient(
                "spotify",
                principal.getName()
        );
        String userId = principal.getAttribute("id");
        String accessToken = authorizedClient.getAccessToken().getTokenValue();
        String url = "http://127.0.0.1:5000/recommend";

        HttpHeaders headers = new HttpHeaders();
        headers.set("Authorization", "Bearer " + accessToken);

        HttpEntity<String> entity = new HttpEntity<>(headers);
        RestTemplate restTemplate = new RestTemplate();

        ResponseEntity<String> response = restTemplate.exchange(url, HttpMethod.GET, entity, String.class);
        return response.getBody();
    }
}
