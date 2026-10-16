import {
  CanActivate,
  ExecutionContext,
  Inject,
  Injectable,
  SetMetadata,
  UnauthorizedException,
} from "@nestjs/common";
import { Reflector } from "@nestjs/core";

export const IS_PUBLIC_KEY = "isPublicEndpoint";
export const Public = () => SetMetadata(IS_PUBLIC_KEY, true);

export interface AuthenticatedPrincipal {
  subject: string;
  providerId?: string;
  scopes: string[];
  claims?: Record<string, unknown>;
}

export abstract class ProviderIdentityPort {
  abstract verifyBearerToken(
    authorizationHeader?: string,
  ): Promise<AuthenticatedPrincipal>;
}

/**
 * Default no-op identity adapter for local challenge execution (Section 2).
 * In production with Keycloak/Zitadel OIDC, replace this binding with OidcJwksProviderIdentityAdapter
 * to validate RS256/ES256 JWTs against the IdP's JWKS endpoint and enforce `provider_id` claim matching.
 */
@Injectable()
export class NoopProviderIdentityAdapter extends ProviderIdentityPort {
  async verifyBearerToken(
    authorizationHeader?: string,
  ): Promise<AuthenticatedPrincipal> {
    if (process.env.AUTH_MODE === "strict" && !authorizationHeader) {
      throw new UnauthorizedException("Missing Authorization Bearer token");
    }
    return {
      subject: "local-provider-client",
      scopes: ["wagering:write", "wagering:read", "wallets:write", "wallets:read"],
    };
  }
}

@Injectable()
export class ProviderAuthGuard implements CanActivate {
  constructor(
    @Inject(Reflector) private readonly reflector: Reflector,
    @Inject(ProviderIdentityPort)
    private readonly identityPort: ProviderIdentityPort,
  ) {}

  async canActivate(context: ExecutionContext): Promise<boolean> {
    const isPublic = this.reflector.getAllAndOverride<boolean>(IS_PUBLIC_KEY, [
      context.getHandler(),
      context.getClass(),
    ]);

    if (isPublic) {
      return true;
    }

    const request = context.switchToHttp().getRequest<{
      headers?: Record<string, string | undefined>;
      principal?: AuthenticatedPrincipal;
    }>();

    const authHeader =
      request.headers?.["authorization"] ?? request.headers?.["Authorization"];
    request.principal = await this.identityPort.verifyBearerToken(authHeader);
    return true;
  }
}
