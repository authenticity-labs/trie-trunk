// Diagnostic only. Uses WebKit's public policy; never loaded by shipped apps.
#import <Cocoa/Cocoa.h>
#import <WebKit/WebKit.h>
#import <objc/runtime.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
static id (*original)(id,SEL,CGRect,WKWebViewConfiguration *);
static id initialize(id self,SEL cmd,CGRect frame,WKWebViewConfiguration *configuration){
    if (@available(macOS 14.0,*)) {
        configuration.preferences.inactiveSchedulingPolicy=WKInactiveSchedulingPolicyNone;
        fprintf(stderr,"TEST: WebKit inactive scheduling = None\n");
    }
    return original(self,cmd,frame,configuration);
}
__attribute__((constructor)) static void scheduling(void){
    if(strcmp(getprogname(),"neutralino")!=0)return;
    if(getenv("SNOOT_TEST_ACTIVITY")) {
        // Keep only this diagnostic process active; allow normal system sleep.
        static id activity;
        activity=[[NSProcessInfo processInfo] beginActivityWithOptions:NSActivityUserInitiatedAllowingIdleSystemSleep reason:@"Native query performance test"];
        (void)activity;
        fprintf(stderr,"TEST: user-initiated activity\n");
    }
    if(getenv("SNOOT_TEST_DEFAULT_SCHEDULING"))return;
    Method method=class_getInstanceMethod([WKWebView class],@selector(initWithFrame:configuration:));
    original=(void *)method_setImplementation(method,(IMP)initialize);
}
