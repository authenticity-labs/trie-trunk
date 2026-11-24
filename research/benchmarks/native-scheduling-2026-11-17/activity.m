// Diagnostic-only host adapter. Public WebKit/Cocoa APIs; no data handling.
#import <Cocoa/Cocoa.h>
#import <WebKit/WebKit.h>
#import <objc/runtime.h>
#include <unistd.h>
@interface QueryActivity : NSObject <WKScriptMessageHandlerWithReply>
@property(nonatomic,weak) WKWebView *view;
@property(nonatomic,strong) NSMutableSet *jobs;
@property(nonatomic,strong) id activity;
@property(nonatomic) WKInactiveSchedulingPolicy original;
@property(nonatomic) NSUInteger starts, ends, peak;
@property(nonatomic) NSUInteger epoch, pending, errors;
@property(nonatomic,copy) NSString *mode;
@end
@implementation QueryActivity
- (void)userContentController:(WKUserContentController *)controller didReceiveScriptMessage:(WKScriptMessage *)message replyHandler:(void (^)(id,NSString *))reply {
    if(!message.frameInfo.isMainFrame){reply(nil,@"Main frame required");return;}
    NSDictionary *body=message.body;NSString *action=body[@"action"];NSNumber *key=body[@"id"];
    if([action isEqual:@"wake"]){
        // Test driver: simulate a new host-originated action after idle. This is
        // outside query timing and does not keep the view alive during a query.
        dispatch_after(dispatch_time(DISPATCH_TIME_NOW,100*NSEC_PER_MSEC),dispatch_get_main_queue(),^{
            [self.view evaluateJavaScript:@"0" completionHandler:^(id result,NSError *error){reply(result,error.localizedDescription);}];
        });return;
    }
    if([action isEqual:@"begin"]){
        if(!key || [self.jobs containsObject:key]){reply(nil,@"Invalid activity identity");return;}
        if(!self.jobs.count){
            self.original=self.view.configuration.preferences.inactiveSchedulingPolicy;
            if(![self.mode isEqual:@"default"]&&![self.mode isEqual:@"task"])self.view.configuration.preferences.inactiveSchedulingPolicy=WKInactiveSchedulingPolicyNone;
            if([self.mode isEqual:@"activity"])self.activity=[[NSProcessInfo processInfo] beginActivityWithOptions:NSActivityUserInitiatedAllowingIdleSystemSleep reason:@"Snoot query diagnostic"];
            if([self.mode isEqual:@"task"]){
                // A host-started asynchronous operation remains pending for this
                // work interval. No timer heartbeat or permanent policy change.
                self.epoch++;self.pending++;
                __weak QueryActivity *owner=self;
                [self.view callAsyncJavaScript:@"globalThis.queryTasks ||= new Map(); return await new Promise(resolve=>queryTasks.set(epoch,resolve));"
                    arguments:@{@"epoch":@(self.epoch)} inFrame:nil inContentWorld:WKContentWorld.defaultClientWorld
                    completionHandler:^(id value,NSError *error){
                        QueryActivity *strong=owner;if(!strong)return;
                        strong.pending--;if(error)strong.errors++;
                        fprintf(stderr,"TASK completed pending=%lu error=%s\n",(unsigned long)strong.pending,error.localizedDescription.UTF8String?:"none");
                    }];
            }
            self.starts++;
        }
        [self.jobs addObject:key];self.peak=MAX(self.peak,self.jobs.count);
    }else if([action isEqual:@"end"]){
        if([self.jobs containsObject:key]){
            [self.jobs removeObject:key];
            if(!self.jobs.count){
                if([self.mode isEqual:@"task"]){
                    [self.view callAsyncJavaScript:@"queryTasks.get(epoch)?.(); queryTasks.delete(epoch); return true;"
                        arguments:@{@"epoch":@(self.epoch)} inFrame:nil inContentWorld:WKContentWorld.defaultClientWorld
                        completionHandler:nil];
                }
                if(self.activity){[[NSProcessInfo processInfo] endActivity:self.activity];self.activity=nil;}
                self.view.configuration.preferences.inactiveSchedulingPolicy=self.original;self.ends++;
            }
        }
    }else if(![action isEqual:@"status"]){reply(nil,@"Unknown activity action");return;}
    NSDictionary *state=@{@"mode":self.mode,@"active":@(self.jobs.count),@"starts":@(self.starts),@"ends":@(self.ends),@"peak":@(self.peak),@"policy":@(self.view.configuration.preferences.inactiveSchedulingPolicy),@"original":@(self.original),@"token":@(self.activity!=nil),@"pending":@(self.pending),@"errors":@(self.errors)};
    if(![action isEqual:@"status"]){
        NSDictionary *entry=@{@"at":@([[NSDate date] timeIntervalSince1970]*1000),@"action":action,@"id":key,@"state":state};
        NSData *data=[NSJSONSerialization dataWithJSONObject:entry options:0 error:nil];
        fprintf(stderr,"ACTIVITY %s\n",[[[NSString alloc] initWithData:data encoding:NSUTF8StringEncoding] UTF8String]);
    }
    reply(state,nil);
}
- (void)dealloc {if(self.activity)[[NSProcessInfo processInfo] endActivity:self.activity];}
@end
static id (*originalInit)(id,SEL,CGRect,WKWebViewConfiguration *);
static id initialize(id self,SEL cmd,CGRect frame,WKWebViewConfiguration *config){
    QueryActivity *owner=[QueryActivity new];owner.jobs=[NSMutableSet new];
    owner.mode=@(getenv("SNOOT_SCHEDULING")?:"default");
    // Control experiment only: compare construction-time policy with scoped updates.
    if([owner.mode isEqual:@"persistent"])config.preferences.inactiveSchedulingPolicy=WKInactiveSchedulingPolicyNone;
    owner.original=config.preferences.inactiveSchedulingPolicy;
    [config.userContentController addScriptMessageHandlerWithReply:owner contentWorld:WKContentWorld.pageWorld name:@"queryActivity"];
    id view=originalInit(self,cmd,frame,config);owner.view=view;
    fprintf(stderr,"TEST: scoped scheduling mode=%s original=%ld\n",owner.mode.UTF8String,(long)owner.original);
    return view;
}
__attribute__((constructor)) static void install(void){
    if(strcmp(getprogname(),"neutralino"))return;
    if(@available(macOS 14.0,*)){
        Method method=class_getInstanceMethod(WKWebView.class,@selector(initWithFrame:configuration:));
        originalInit=(void *)method_setImplementation(method,(IMP)initialize);
    }
}
